use std::sync::Arc;

use super::{AnnouncementReadRepository, AnnouncementWriteRepository, GatewayDataState};

impl GatewayDataState {
    pub(crate) fn with_announcement_reader_for_tests(
        repository: Arc<dyn AnnouncementReadRepository>,
    ) -> Self {
        Self {
            announcement_reader: Some(repository),
            ..Self::default()
        }
    }

    pub(crate) fn with_announcement_repository_for_tests<T>(repository: Arc<T>) -> Self
    where
        T: aether_data::repository::announcements::AnnouncementReadRepository
            + aether_data::repository::announcements::AnnouncementWriteRepository
            + 'static,
    {
        let announcement_reader: Arc<dyn AnnouncementReadRepository> = repository.clone();
        let announcement_writer: Arc<dyn AnnouncementWriteRepository> = repository;
        Self {
            announcement_reader: Some(announcement_reader),
            announcement_writer: Some(announcement_writer),
            ..Self::default()
        }
    }
}
