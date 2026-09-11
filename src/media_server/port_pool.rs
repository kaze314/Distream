use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

pub const PORT_BASE: u16 = 34662;
pub const PORT_COUNT: u16 = 100;

#[derive(Clone)]
pub struct PortPool {
    // Two peers on one machine handing out the same numbers would fight over
    // the same sockets, so the range is per process.
    base: u16,

    ports: Arc<Mutex<Vec<u16>>>,

    //  plain usize belongs to whichever clone holds it, so two
    // clones would walk their own cursors and hand out the same port.
    current: Arc<AtomicUsize>
}


impl PortPool {
    pub fn new(base: u16) -> PortPool {
        let mut ports = Vec::new();
        ports.extend_from_slice(&[0; PORT_COUNT as usize]);
        let current = Arc::new(AtomicUsize::new(0));
        PortPool { base, ports: Arc::new(Mutex::new(ports)), current }
    }

    // None when every port is taken.
    pub fn get_new(&self) -> Option<usize> {
        let count = PORT_COUNT as usize;
        let mut ports = self.ports.lock().unwrap();
        let start = self.current.load(Ordering::Relaxed);

        for step in 0..count {
            let index = (start + step) % count;
            if ports[index] != 0 {
                continue;
            }

            ports[index] = 1;
            self.current.store((index + 1) % count, Ordering::Relaxed);

            return Some(index + self.base as usize);
        }

        println!("[ports] all {count} in use");
        None
    }

    pub fn release(&self, port: usize) {
        let mut ports = self.ports.lock().unwrap();
        let index = port - self.base as usize;

        ports[index] = 0;
    }
}