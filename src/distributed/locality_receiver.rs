use async_trait::async_trait;

use crate::semantics::distributed::localisation::LocalitySpec;

#[async_trait(?Send)]
pub trait LocalityReceiver {
    async fn receive(&self) -> Result<impl LocalitySpec + 'static, Box<dyn std::error::Error>>;
}
