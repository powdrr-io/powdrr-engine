use crate::service_impl_provider::SERVICE_IMPL;
use std::sync::Once;
use std::time::Duration;

static CHECKPOINT_UPDATER_STARTED: Once = Once::new();

pub(crate) fn ensure_checkpoint_updater_started() {
    CHECKPOINT_UPDATER_STARTED.call_once(|| {
        std::thread::Builder::new()
            .name("checkpoint-updater".to_string())
            .spawn(|| {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("failed to build checkpoint updater runtime");
                runtime.block_on(async {
                    loop {
                        let work_done = match SERVICE_IMPL.update_all_checkpoints().await {
                            Ok(checkpoint_work_done) => checkpoint_work_done,
                            Err(error) => {
                                tracing::error!("Error updating checkpoints: {}", error);
                                false
                            }
                        };
                        if !work_done {
                            tokio::time::sleep(Duration::from_millis(1000)).await;
                        }
                    }
                });
            })
            .expect("failed to start checkpoint updater thread");
    });
}
