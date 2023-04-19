use hive.ccma_analytics;
CALL system.sync_partition_metadata('ccma_analytics', 'enterprise_gfk_pgfk_csv', 'ADD', true);
CALL system.sync_partition_metadata('ccma_analytics', 'enterprise_gfk_vgfk_csv', 'ADD', true);