
pub struct StreamerQueue<T> {
    index: usize,
    max_repeats: usize,
    queue: Vec<(usize, T)>
}

impl<T: Clone> StreamerQueue<T> {
    pub fn new(max_repeats: usize) -> StreamerQueue<T> {
        StreamerQueue {
            index: 0,
            max_repeats: max_repeats,
            queue: Vec::new(),
        }
    }

    pub fn push_back(&mut self, item: T) {
        self.queue.push((0, item));
    }

    // It's not guaranteed there will be enough streamers, so
    // instead of permanently removing a streamer from the queue, keep it.
    // If there are not enough streamers for the demand repeat the queue.
    pub fn pop_front(&mut self) -> Option<&T> {
        let queue_len = self.queue.len();
        if let Some(entry) = self.queue.get_mut(self.index) {
            entry.0 += 1;

            // Update repeats.
            // If the streamer has reached the
            // max repeats increment the index.
            if entry.0 >= self.index {
                self.index += 1;
                if self.index >= queue_len {
                    self.index = 0;
                }
            }

            return Some(&entry.1);
        }

        None
    }
}