use std::sync::atomic::{AtomicUsize, Ordering};

macro_rules! atomic_usize_stats {
    (pub struct $struct_name:ident {
        $(pub $field_name:ident: AtomicUsize,)*
    }) => {
        pub struct $struct_name {
            $(pub $field_name: AtomicUsize,)*
        }

        impl Default for $struct_name {
            fn default() -> Self {
                Self {
                    $($field_name: AtomicUsize::new(0),)*
                }
            }
        }

        impl $struct_name {
            pub fn to_lines_str(&self) -> Vec<(String, usize)> {
                vec![
                    $((stringify!($field_name).to_string(), self.$field_name.load(Ordering::Relaxed))),*
                ]
            }
        }
    }
}

atomic_usize_stats! {
    pub struct MigrationStats {
        pub migrating_src_redis_conn: AtomicUsize,
        pub migrating_dst_redis_conn: AtomicUsize,
        pub migrating_active_sync_lock_success: AtomicUsize,
        pub migrating_active_sync_lock_failed: AtomicUsize,
        pub migrating_scan_lock_success: AtomicUsize,
        pub migrating_scan_lock_failed: AtomicUsize,
        pub importing_blocking_migration_commands: AtomicUsize,
        pub importing_non_blocking_migration_commands: AtomicUsize,
        pub importing_umsync_lock_success: AtomicUsize,
        pub importing_umsync_lock_failed: AtomicUsize,
        pub importing_umsync_lock_failed_again: AtomicUsize,
        pub importing_umsync_failed: AtomicUsize,
        pub importing_dst_key_existed: AtomicUsize,
        pub importing_dst_key_not_existed: AtomicUsize,
        pub importing_lock_success: AtomicUsize,
        pub importing_lock_failed: AtomicUsize,
        pub importing_resend_exists: AtomicUsize,
        pub importing_resend_exists_failed: AtomicUsize,
        pub importing_lock_loop_retry: AtomicUsize,
        pub importing_src_key_existed: AtomicUsize,
        pub importing_src_key_not_existed: AtomicUsize,
        pub importing_src_failed: AtomicUsize,
    }
}
