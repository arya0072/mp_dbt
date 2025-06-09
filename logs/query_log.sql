-- created_at: 2025-06-05T03:01:39.967566115+00:00
-- dialect: bigquery
-- node_id: not available
-- desc: Get table schema
SELECT column_name, data_type, is_nullable FROM `mitraprodin-data-warehouse`.`PlantControlGianyarDps`.INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'JO_PostedBy' UNION ALL SELECT * FROM (SELECT '', '', '' FROM `mitraprodin-data-warehouse`.`PlantControlGianyarDps`.JO_PostedBy LIMIT 0);
-- created_at: 2025-06-05T03:01:39.968530737+00:00
-- dialect: bigquery
-- node_id: not available
-- desc: Get table schema
SELECT column_name, data_type, is_nullable FROM `mitraprodin-data-warehouse`.`PlantControlGianyarDps`.INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'conversions_table' UNION ALL SELECT * FROM (SELECT '', '', '' FROM `mitraprodin-data-warehouse`.`PlantControlGianyarDps`.conversions_table LIMIT 0);
-- created_at: 2025-06-05T03:01:42.366785730+00:00
-- dialect: bigquery
-- node_id: not available
-- desc: Ensure schema exists
CREATE SCHEMA IF NOT EXISTS `mitraprodin-data-warehouse.mp_infor`;
-- created_at: 2025-06-05T03:01:42.369944015+00:00
-- dialect: bigquery
-- node_id: not available
-- desc: Ensure schema exists
CREATE SCHEMA IF NOT EXISTS `mitraprodin-data-warehouse.mp_staging`;
-- created_at: 2025-06-05T03:01:44.127016858+00:00
-- dialect: bigquery
-- node_id: model.mp_dbt.META_TotalHours_v
-- desc: get_relation adapter call
SELECT table_catalog,
                    table_schema,
                    table_name,
                    table_type
                FROM `mitraprodin-data-warehouse`.`mp_staging`.INFORMATION_SCHEMA.TABLES
                WHERE table_name = 'META_TotalHours_v';
