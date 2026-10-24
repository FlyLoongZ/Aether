use std::sync::Arc;

use aether_data_contracts::repository::candidates::RequestCandidateRepository;
use aether_data_contracts::repository::video_tasks::VideoTaskRepository;

use super::{
    AuthApiKeyReadRepository, GatewayDataState, ProviderCatalogReadRepository,
    VideoTaskReadRepository, VideoTaskWriteRepository,
};

impl GatewayDataState {
    pub(crate) fn with_video_task_reader_for_tests(
        repository: Arc<dyn VideoTaskReadRepository>,
    ) -> Self {
        Self {
            video_task_reader: Some(repository),
            ..Self::default()
        }
    }

    pub(crate) fn with_auth_and_video_task_repository_for_tests<T>(
        auth_repository: Arc<dyn AuthApiKeyReadRepository>,
        repository: Arc<T>,
    ) -> Self
    where
        T: VideoTaskRepository + 'static,
    {
        let video_task_reader: Arc<dyn VideoTaskReadRepository> = repository.clone();
        let video_task_writer: Arc<dyn VideoTaskWriteRepository> = repository;

        Self {
            auth_api_key_reader: Some(auth_repository),
            video_task_reader: Some(video_task_reader),
            video_task_writer: Some(video_task_writer),
            ..Self::default()
        }
    }

    pub(crate) fn attach_video_task_repository_for_tests<T>(mut self, repository: Arc<T>) -> Self
    where
        T: VideoTaskRepository + 'static,
    {
        self.video_task_reader = Some(repository.clone());
        self.video_task_writer = Some(repository);
        self
    }

    pub(crate) fn with_video_task_repository_for_tests<T>(repository: Arc<T>) -> Self
    where
        T: VideoTaskRepository + 'static,
    {
        super::GatewayDataStateFixture::new()
            .with_video_task_repository(repository)
            .build()
    }

    pub(crate) fn with_video_task_repository_and_provider_transport_for_tests<T>(
        repository: Arc<T>,
        provider_catalog_repository: Arc<dyn ProviderCatalogReadRepository>,
        encryption_key: impl Into<String>,
    ) -> Self
    where
        T: VideoTaskRepository + 'static,
    {
        super::GatewayDataStateFixture::new()
            .with_encryption_key(encryption_key)
            .with_provider_catalog_reader(provider_catalog_repository)
            .with_video_task_repository(repository)
            .build()
    }

    pub(crate) fn with_video_task_and_request_candidate_repository_for_tests<T, U>(
        repository: Arc<T>,
        request_candidate_repository: Arc<U>,
    ) -> Self
    where
        T: VideoTaskRepository + 'static,
        U: RequestCandidateRepository + 'static,
    {
        super::GatewayDataStateFixture::new()
            .with_request_candidate_repository(request_candidate_repository)
            .with_video_task_repository(repository)
            .build()
    }

    pub(crate) fn with_video_task_provider_transport_and_request_candidate_repository_for_tests<
        T,
        U,
        V,
    >(
        repository: Arc<T>,
        provider_catalog_repository: Arc<U>,
        request_candidate_repository: Arc<V>,
        encryption_key: impl Into<String>,
    ) -> Self
    where
        T: VideoTaskRepository + 'static,
        U: ProviderCatalogReadRepository + 'static,
        V: RequestCandidateRepository + 'static,
    {
        super::GatewayDataStateFixture::new()
            .with_encryption_key(encryption_key)
            .with_provider_catalog_reader(provider_catalog_repository)
            .with_request_candidate_repository(request_candidate_repository)
            .with_video_task_repository(repository)
            .build()
    }
}
