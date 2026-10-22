#[cfg(any(test, feature = "test-support"))]
mod memory;

pub use aether_data_contracts::repository::announcements::{
    AnnouncementListQuery, AnnouncementReadRepository, AnnouncementWriteRepository,
    CreateAnnouncementRecord, StoredAnnouncement, StoredAnnouncementPage, StoredUserAnnouncement,
    StoredUserAnnouncementPage, UpdateAnnouncementRecord, UserAnnouncementListQuery,
};
#[cfg(feature = "postgres")]
pub use aether_data_postgres::SqlxAnnouncementReadRepository;
#[cfg(any(test, feature = "test-support"))]
pub use memory::InMemoryAnnouncementReadRepository;
