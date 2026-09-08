use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

const PORT_BASE: u16 = 34662;
const PORT_COUNT: u16 = 100;

#[derive(Clone)]
pub struct PortPool {
    ports: Arc<Mutex<Vec<u16>>>,
    current: usize
}


impl PortPool {
    pub fn new() -> PortPool {
        let ports = (PORT_BASE..PORT_BASE + PORT_COUNT).collect();
        let current = 0;
        PortPool { ports: Arc::new(Mutex::new(ports)), current }
    }

    pub fn get_new(&self) -> usize {
        loop {
            let mut ports = self.ports.lock().unwrap();
            let index = self.current % PORT_COUNT as usize;

            if ports[index] == 0 {
                ports[index] = 1;

                return index + PORT_BASE as usize;
            }
        }
    }

    pub fn release(&self, port: usize) {
        let mut ports = self.ports.lock().unwrap();
        let index = port - PORT_BASE as usize;

        ports[index] = 0;
    }
}