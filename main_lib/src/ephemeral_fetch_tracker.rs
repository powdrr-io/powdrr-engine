use std::collections::HashMap;
use crate::peers::CheckpointDescriptor;
use crate::state_provider::ServiceApiError;

pub(crate) struct EphemeralFetchTracker {
    next_target: HashMap<Option<String>, HashMap<String, String>>,
    current_target: HashMap<Option<String>, HashMap<String, String>>,
}

impl EphemeralFetchTracker {
    pub fn new() -> Self {
        EphemeralFetchTracker{
            next_target: Default::default(),
            current_target: Default::default(),
        }
    }

    pub(crate) async fn get_latest_target_checkpoint(&self, table_name: &String, extension: Option<String>) -> Result<Option<String>, ServiceApiError>{
        match self.current_target.get(&extension) {
            Some(target) => Ok(target.get(table_name).cloned()),
            None => Ok(None)
        }
    }

    pub(crate) async fn set_target_checkpoints(&mut self, descriptors: &Vec<CheckpointDescriptor>, extension: Option<String>) -> Result<(), ServiceApiError> {
        if !self.current_target.contains_key(&extension) {
            self.current_target.insert(extension.clone(), HashMap::new());
        }
        let target_map = self.current_target.get_mut(&extension).unwrap();
        for descriptor in descriptors {
            match target_map.get(&descriptor.table_name) {
                Some(value) => {
                    if value < &descriptor.checkpoint_id {
                        target_map.insert(descriptor.table_name.clone(), descriptor.checkpoint_id.clone());
                    }
                },
                None => {
                    target_map.insert(descriptor.table_name.clone(), descriptor.checkpoint_id.clone());
                }
            }
        }
        Ok(())
    }

    pub async fn get_next_prefetch_checkpoints(&mut self, extensions: Option<String>) -> Result<Vec<CheckpointDescriptor>, ServiceApiError> {
        let target_map = match self.next_target.get(&extensions) {
            Some(target) => target,
            None => return Ok(vec!())
        };

        Ok(target_map.iter().map(|(key, value)| CheckpointDescriptor{ table_name: key.clone(), checkpoint_id: value.clone()}).collect())
    }

    pub async fn set_next_prefetch_checkpoints(&mut self, table_name: &String, extension: Option<String>, checkpoint_id: &String) -> Result<(), ServiceApiError> {
        if !self.next_target.contains_key(&extension) {
            self.next_target.insert(extension.clone(), HashMap::new());
        }
        let target_map = self.next_target.get_mut(&extension).unwrap();
        target_map.insert(table_name.clone(), checkpoint_id.clone());
        Ok(())
    }
}
