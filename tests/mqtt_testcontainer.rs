use testcontainers_modules::testcontainers::core::IntoContainerPort;
use testcontainers_modules::testcontainers::core::{ContainerPort, WaitFor};
use testcontainers_modules::testcontainers::Image;
use testcontainers_modules::testcontainers::core::wait::LogWaitStrategy;

#[derive(Debug, Clone)]
pub struct EmqxImage {
    ports: Vec<ContainerPort>,
}

impl Default for EmqxImage {
    fn default() -> Self {
        Self {
            ports: vec![1883_u16.tcp()],
        }
    }
}

impl Image for EmqxImage {
    fn name(&self) -> &str {
        "emqx/emqx"
    }

    fn tag(&self) -> &str {
        "5.8.3"
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        vec![WaitFor::log(LogWaitStrategy::stdout(
            "EMQX 5.8.3 is running now!",
        ))]
    }

    fn expose_ports(&self) -> &[ContainerPort] {
        &self.ports
    }
}