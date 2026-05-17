mod logging;
mod stats;

use nix::sys::socket::SockaddrLike;
use socket2::{Domain, Protocol, Socket, Type};
use stats::Stats;
use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddrV4};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

const MDNS_ADDR: Ipv4Addr = Ipv4Addr::new(224, 0, 0, 251);
const MDNS_PORT: u16 = 5353;
const DEDUP_TTL: Duration = Duration::from_secs(1);

type PacketKey = [u8; 32];

#[derive(Clone)]
struct Iface {
    name: String,
    addr: Ipv4Addr,
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
        .filter_map(|name| match get_iface_addr(name) {
            Some(addr) => {
                info!(iface = %name, addr = %addr, "interface ready");
                Some(Iface {
                    name: name.clone(),
                    addr,
                })
            }
            None => {
                warn!(iface = %name, "could not resolve interface address, skipping");
                None
            }
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

    let senders: Arc<Vec<(Iface, Arc<Socket>)>> = Arc::new(
        ifaces.iter().filter_map(|iface| {
            match make_sender(&iface.addr) {
                Ok(sock) => Some((iface.clone(), Arc::new(sock))),
                Err(e) => {
                    warn!(iface = %iface.name, error = %e, "failed to create sender socket, skipping");
                    None
                }
            }
        }).collect()
    );

    let mut handles = vec![];
    for iface in &ifaces {
        let iface = iface.clone();
        let senders = Arc::clone(&senders);
        let seen = Arc::clone(&seen);
        let stats = Arc::clone(&stats);

        handles.push(tokio::spawn(async move {
            listen_and_repeat(iface, senders, seen, stats).await;
        }));
    }

    let stats_log = Arc::clone(&stats);
    let stats_interval_secs: u64 = std::env::var("STATS_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(60);

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(stats_interval_secs));
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
    for h in handles {
        let _ = h.await;
    }
}

async fn listen_and_repeat(
    iface: Iface,
    senders: Arc<Vec<(Iface, Arc<Socket>)>>,
    seen: Arc<Mutex<HashMap<PacketKey, Instant>>>,
    stats: Arc<Stats>,
) {
    let sock = match make_listener(&iface.addr) {
        Ok(s) => s,
        Err(e) => {
            error!(
                iface = %iface.name,
                error = %e,
                "failed to create listener socket — is avahi or systemd-resolved holding port 5353? \
                 try: ss -ulnp | grep 5353"
            );
            return;
        }
    };

    let std_sock: std::net::UdpSocket = sock.into();
    if let Err(e) = std_sock.set_nonblocking(true) {
        error!(iface = %iface.name, error = %e, "set_nonblocking failed");
        return;
    }

    let udp = match tokio::net::UdpSocket::from_std(std_sock) {
        Ok(s) => s,
        Err(e) => {
            error!(iface = %iface.name, error = %e, "failed to convert to tokio socket");
            return;
        }
    };

    info!(iface = %iface.name, addr = %iface.addr, "listener started");

    let mut buf = vec![0u8; 9000];
    loop {
        let (len, src) = match udp.recv_from(&mut buf).await {
            Ok(v) => v,
            Err(e) => {
                warn!(iface = %iface.name, error = %e, "recv error");
                stats.inc_errors();
                // Small backoff to avoid a tight error loop
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
        };

        stats.inc_received();

        let pkt = &buf[..len];

        let mut key = [0u8; 32];
        key[..len.min(32)].copy_from_slice(&pkt[..len.min(32)]);

        {
            let mut map = seen.lock().await;
            let now = Instant::now();
            if let Some(t) = map.get(&key) {
                if t.elapsed() < DEDUP_TTL {
                    debug!(
                        iface = %iface.name,
                        src = %src,
                        bytes = len,
                        "packet deduplicated"
                    );
                    stats.inc_deduplicated();
                    continue;
                }
            }
            map.insert(key, now);
        }

        let src_ip = match src.ip() {
            IpAddr::V4(ip) => ip,
            _ => continue,
        };

        debug!(
            iface = %iface.name,
            src   = %src_ip,
            bytes = len,
            "received mDNS packet, forwarding"
        );

        let dest = std::net::SocketAddr::V4(SocketAddrV4::new(MDNS_ADDR, MDNS_PORT));
        let mut fwd_count = 0u32;

        for (other, sender) in senders.iter() {
            if other.name == iface.name {
                continue;
            }
            match sender.send_to(pkt, &dest.into()) {
                Ok(_) => {
                    fwd_count += 1;
                    stats.inc_forwarded();
                }
                Err(e) => {
                    warn!(
                        from  = %iface.name,
                        to    = %other.name,
                        error = %e,
                        "forward error"
                    );
                    stats.inc_errors();
                }
            }
        }

        debug!(
            iface        = %iface.name,
            src          = %src_ip,
            forwarded_to = fwd_count,
            "done"
        );
    }
}

fn make_listener(addr: &Ipv4Addr) -> Result<Socket, std::io::Error> {
    let sock = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    sock.set_reuse_address(true)?;
    sock.set_reuse_port(true)?;
    // Bind to the multicast address rather than UNSPECIFIED so we can
    // coexist with avahi / systemd-resolved which bind to 0.0.0.0:5353.
    // The kernel still delivers multicast copies to all matching sockets.
    sock.bind(&SocketAddrV4::new(MDNS_ADDR, MDNS_PORT).into())?;
    sock.join_multicast_v4(&MDNS_ADDR, addr)?;
    sock.set_multicast_loop_v4(false)?;
    Ok(sock)
}

fn make_sender(addr: &Ipv4Addr) -> Result<Socket, std::io::Error> {
    let sock = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    sock.set_reuse_address(true)?;
    sock.bind(&SocketAddrV4::new(*addr, 0).into())?;
    sock.set_multicast_if_v4(addr)?;
    sock.set_multicast_ttl_v4(1)?;
    sock.set_nonblocking(true)?;
    Ok(sock)
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
