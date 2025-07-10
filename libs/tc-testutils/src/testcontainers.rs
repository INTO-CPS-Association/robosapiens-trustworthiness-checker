use std::mem;

use async_compat::Compat as TokioCompat;
use testcontainers_modules::testcontainers::{ContainerAsync as ContainerAsyncTokio, Image};

pub struct ContainerAsync<T: Image> {
    inner: Option<ContainerAsyncTokio<T>>,
}

impl<T: Image> ContainerAsync<T> {
    pub fn new(inner: ContainerAsyncTokio<T>) -> Self {
        Self { inner: Some(inner) }
    }

    pub async fn get_host_port_ipv4(
        &self,
        port: u16,
    ) -> Result<u16, testcontainers_modules::testcontainers::TestcontainersError> {
        TokioCompat::new(self.inner.as_ref().unwrap().get_host_port_ipv4(port)).await
    }
}

impl<T: Image> Drop for ContainerAsync<T> {
    fn drop(&mut self) {
        let inner = mem::take(&mut self.inner);
        TokioCompat::new(async move { mem::drop(inner) });
    }
}
