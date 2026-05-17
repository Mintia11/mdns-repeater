mod logging;
mod stats;

use socket2::{Domain, Protocol, Socket, Type};
use stats::Stats;
use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::os::unix::io::{AsRawFd, FromRawFd};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

const MDNS_ADDR: Ipv4Addr = Ipv4Addr::new(224, 0, 0, 251);
const MDNS_PORT: u16 = 5353;
const DEDUP_TTL: Duration = Duration::from_secs(1);

type PacketKey = [u8; 32];

#[derive(Clone, Debug)]
struct Iface {
    name: String,
    addr: Ipv4Addr,
    index: u32,
}

#[tokio::main]
async fn main() {
    logging::init();

    info!(
        version = env!("CARGO_PKG_VERSION"),
        pid = std::process::id(),
        "mdns-repeater starting"
    );

    let mut iface_names: Vec<String> = std::env::var("INTERFACES")
        .unwrap_or_default()
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    if iface_names.is_empty() {
        info!("INTERFACES not set, auto-discovering Docker bridges");
        iface_names = discover_docker_bridges();

        if let Ok(host) = std::env::var("HOST_IFACE") {
            info!(iface = %host, "using HOST_IFACE env var");
            iface_names.push(host);
        } else if let Some(nic) = discover_host_nic() {
            info!(iface = %nic, "auto-detected host NIC");
            iface_names.push(nic);
        } else {
            warn!("could not auto-detect host NIC — set HOST_IFACE env var explicitly");
        }
    }

    info!(interfaces = ?iface_names, "resolving interfaces");

    let ifaces: Vec<Iface> = iface_names
        .iter()
        .filter_map(|name| {
            let addr = match get_iface_addr(name) {
                Some(a) => a,
                None => {
                    warn!(iface = %name, "could not resolve address, skipping");
                    return None;
                }
            };
            let index = match get_iface_index(name) {
                Some(i) => i,
                None => {
                    warn!(iface = %name, "could not resolve interface index, skipping");
                    return None;
                }
            };
            info!(iface = %name, %addr, index, "interface ready");
            Some(Iface {
                name: name.clone(),
                addr,
                index,
            })
        })
        .collect();

    if ifaces.len() < 2 {
        error!(
            found = ifaces.len(),
            "need at least 2 reachable interfaces, aborting"
        );
        std::process::exit(1);
    }

    info!(
        count = ifaces.len(),
        "all interfaces resolved, starting repeater"
    );

    let seen: Arc<Mutex<HashMap<PacketKey, Instant>>> = Arc::new(Mutex::new(HashMap::new()));
    let stats = Arc::new(Stats::default());

    // One sender socket per interface, bound to that interface's address
    let senders: Arc<Vec<(Iface, Arc<Socket>)>> = Arc::new(
        ifaces
            .iter()
            .filter_map(|iface| match make_sender(&iface.addr) {
                Ok(sock) => Some((iface.clone(), Arc::new(sock))),
                Err(e) => {
                    warn!(iface = %iface.name, error = %e, "failed to create sender, skipping");
                    None
                }
            })
            .collect(),
    );

    let ifaces = Arc::new(ifaces);

    // Single raw listener instead of one UDP socket per interface
    tokio::spawn(run_listener(
        Arc::clone(&ifaces),
        Arc::clone(&senders),
        Arc::clone(&seen),
        Arc::clone(&stats),
    ));

    // Periodic stats
    let stats_log = Arc::clone(&stats);
    let stats_interval: u64 = std::env::var("STATS_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(60);

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(stats_interval));
        loop {
            interval.tick().await;
            let s = stats_log.snapshot();
            info!(
                received = s.received,
                forwarded = s.forwarded,
                deduplicated = s.deduplicated,
                errors = s.errors,
                "stats"
            );
        }
    });

    // Dedup cache cleanup
    let seen_cleanup = Arc::clone(&seen);
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        loop {
            interval.tick().await;
            let mut map = seen_cleanup.lock().await;
            let before = map.len();
            map.retain(|_, t| t.elapsed() < DEDUP_TTL * 10);
            let removed = before - map.len();
            if removed > 0 {
                debug!(removed, remaining = map.len(), "dedup cache pruned");
            }
        }
    });

    info!("repeater running — press ctrl-c to stop");

    // Park the main task forever
    tokio::signal::ctrl_c().await.ok();
    info!("shutting down");
}

async fn run_listener(
    ifaces: Arc<Vec<Iface>>,
    senders: Arc<Vec<(Iface, Arc<Socket>)>>,
    seen: Arc<Mutex<HashMap<PacketKey, Instant>>>,
    stats: Arc<Stats>,
) {
    let sock = match make_raw_listener() {
        Ok(s) => s,
        Err(e) => {
            error!(error = %e, "failed to create raw socket — is NET_RAW capability set?");
            return;
        }
    };

    let fd = sock.as_raw_fd();

    // Convert to std UdpSocket just so tokio can poll readability on the fd.
    // We never actually call recv on this — we use recvmsg directly.
    let std_sock: std::net::UdpSocket = unsafe { FromRawFd::from_raw_fd(fd) };
    std_sock.set_nonblocking(true).unwrap();
    let udp = tokio::net::UdpSocket::from_std(std_sock).unwrap();

    // Prevent the socket2::Socket from closing fd when it drops
    std::mem::forget(sock);

    info!("raw listener started");

    let mut buf = vec![0u8; 65535];

    loop {
        // Wait until the fd is readable, then do a non-blocking recvmsg
        if let Err(e) = udp.readable().await {
            error!(error = %e, "readable() error");
            break;
        }

        let (len, iface_index) = match recv_raw_with_iface(fd, &mut buf) {
            Ok(v) => v,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(e) => {
                warn!(error = %e, "recvmsg error");
                stats.inc_errors();
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };

        // --- Parse IP header ---
        if len < 20 {
            continue;
        }
        let ip_header_len = ((buf[0] & 0x0f) as usize) * 4;
        if len < ip_header_len + 8 {
            continue;
        }

        // Only care about packets destined for 224.0.0.251
        let dst_ip = Ipv4Addr::new(buf[16], buf[17], buf[18], buf[19]);
        if dst_ip != MDNS_ADDR {
            continue;
        }

        // --- Parse UDP header ---
        let udp_start = ip_header_len;
        let dst_port = u16::from_be_bytes([buf[udp_start + 2], buf[udp_start + 3]]);
        if dst_port != MDNS_PORT {
            continue;
        }

        // --- mDNS payload ---
        let payload_start = udp_start + 8;
        if len <= payload_start {
            continue;
        }
        let pkt = buf[payload_start..len].to_vec();

        stats.inc_received();

        // Dedup on first 32 bytes of payload
        let mut key = [0u8; 32];
        key[..pkt.len().min(32)].copy_from_slice(&pkt[..pkt.len().min(32)]);

        {
            let mut map = seen.lock().await;
            let now = Instant::now();
            if let Some(t) = map.get(&key) {
                if t.elapsed() < DEDUP_TTL {
                    debug!(iface_index, "deduplicated");
                    stats.inc_deduplicated();
                    continue;
                }
            }
            map.insert(key, now);
        }

        let in_name = ifaces
            .iter()
            .find(|i| i.index == iface_index)
            .map(|i| i.name.as_str())
            .unwrap_or("unknown");

        debug!(iface = %in_name, bytes = pkt.len(), "received mDNS, forwarding");

        let dest = std::net::SocketAddr::V4(SocketAddrV4::new(MDNS_ADDR, MDNS_PORT));
        let mut fwd_count = 0u32;

        for (iface, sender) in senders.iter() {
            if iface.index == iface_index {
                continue; // don't echo back to the source interface
            }
            match sender.send_to(&pkt, &dest.into()) {
                Ok(_) => {
                    fwd_count += 1;
                    stats.inc_forwarded();
                    debug!(to = %iface.name, "forwarded");
                }
                Err(e) => {
                    warn!(to = %iface.name, error = %e, "forward error");
                    stats.inc_errors();
                }
            }
        }

        debug!(iface = %in_name, forwarded_to = fwd_count, "done");
    }
}

fn make_raw_listener() -> std::io::Result<Socket> {
    // IPPROTO_UDP raw socket — receives all UDP packets, we filter in userspace
    let sock = Socket::new(Domain::IPV4, Type::RAW, Some(Protocol::UDP))?;
    sock.set_nonblocking(true)?;

    // Enable IP_PKTINFO so recvmsg tells us which interface each packet arrived on
    unsafe {
        let one: libc::c_int = 1;
        let ret = libc::setsockopt(
            sock.as_raw_fd(),
            libc::IPPROTO_IP,
            libc::IP_PKTINFO,
            &one as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
        if ret != 0 {
            return Err(std::io::Error::last_os_error());
        }
    }

    Ok(sock)
}

fn make_sender(addr: &Ipv4Addr) -> std::io::Result<Socket> {
    let sock = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    sock.set_reuse_address(true)?;
    sock.bind(&SocketAddrV4::new(*addr, 0).into())?;
    sock.set_multicast_if_v4(addr)?;
    sock.set_multicast_ttl_v4(1)?;
    sock.set_nonblocking(true)?;
    Ok(sock)
}

fn recv_raw_with_iface(
    fd: std::os::unix::io::RawFd,
    buf: &mut [u8],
) -> std::io::Result<(usize, u32)> {
    unsafe {
        let mut iov = libc::iovec {
            iov_base: buf.as_mut_ptr() as *mut libc::c_void,
            iov_len: buf.len(),
        };

        // Control buffer for IP_PKTINFO cmsg
        let mut ctrl = [0u8; 256];
        let mut src: libc::sockaddr_in = std::mem::zeroed();

        let mut msg = libc::msghdr {
            msg_name: &mut src as *mut _ as *mut libc::c_void,
            msg_namelen: std::mem::size_of::<libc::sockaddr_in>() as u32,
            msg_iov: &mut iov,
            msg_iovlen: 1,
            msg_control: ctrl.as_mut_ptr() as *mut libc::c_void,
            msg_controllen: ctrl.len(),
            msg_flags: 0,
        };

        let n = libc::recvmsg(fd, &mut msg, libc::MSG_DONTWAIT);
        if n < 0 {
            return Err(std::io::Error::last_os_error());
        }

        let mut iface_index = 0u32;
        let mut cmsg = libc::CMSG_FIRSTHDR(&msg);
        while !cmsg.is_null() {
            let hdr = &*cmsg;
            if hdr.cmsg_level == libc::IPPROTO_IP && hdr.cmsg_type == libc::IP_PKTINFO {
                let info = libc::CMSG_DATA(cmsg) as *const libc::in_pktinfo;
                iface_index = (*info).ipi_ifindex as u32;
            }
            cmsg = libc::CMSG_NXTHDR(&msg, cmsg);
        }

        Ok((n as usize, iface_index))
    }
}

fn get_iface_addr(name: &str) -> Option<Ipv4Addr> {
    use nix::ifaddrs::getifaddrs;
    use nix::sys::socket::AddressFamily;
    let addrs = getifaddrs().ok()?;
    for ifaddr in addrs {
        if ifaddr.interface_name != name {
            continue;
        }
        if let Some(addr) = ifaddr.address {
            if addr.family() == Some(AddressFamily::Inet) {
                if let Some(sin) = addr.as_sockaddr_in() {
                    return Some(Ipv4Addr::from(sin.ip()));
                }
            }
        }
    }
    None
}

fn get_iface_index(name: &str) -> Option<u32> {
    let cname = std::ffi::CString::new(name).ok()?;
    let idx = unsafe { libc::if_nametoindex(cname.as_ptr()) };
    if idx == 0 { None } else { Some(idx) }
}

fn discover_docker_bridges() -> Vec<String> {
    use nix::ifaddrs::getifaddrs;
    use nix::sys::socket::AddressFamily;
    let mut names = std::collections::HashSet::new();
    if let Ok(addrs) = getifaddrs() {
        for ifaddr in addrs {
            let name = &ifaddr.interface_name;
            if (name.starts_with("br-") || name == "docker0")
                && ifaddr.address.map(|a| a.family()) == Some(Some(AddressFamily::Inet))
            {
                names.insert(name.clone());
            }
        }
    }
    let mut v: Vec<_> = names.into_iter().collect();
    v.sort();
    v
}

fn discover_host_nic() -> Option<String> {
    use nix::ifaddrs::getifaddrs;
    use nix::sys::socket::AddressFamily;
    let skip_prefixes = ["lo", "br-", "docker", "veth", "virbr", "tun", "tap"];
    let addrs = getifaddrs().ok()?;
    for ifaddr in addrs {
        let name = &ifaddr.interface_name;
        if skip_prefixes.iter().any(|p| name.starts_with(p)) {
            continue;
        }
        if ifaddr.address.map(|a| a.family()) == Some(Some(AddressFamily::Inet)) {
            return Some(name.clone());
        }
    }
    None
}
