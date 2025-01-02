-- Company Master unique domain insertion

drop TABLE if EXISTS numbered_data;
create temp table numbered_data AS
	SELECT
		updated_domain,
		source_name,
		company_domain
	FROM
		stg_da.tbl_da_co_domain_merge_final 
	WHERE
		COALESCE ( updated_domain, '' ) <> ''
	GROUP BY
		updated_domain,
		source_name,
		company_domain;
		
drop TABLE if EXISTS grouped_data;
create temp table grouped_data AS
	SELECT
		updated_domain,
		source_name,
		jsonb_agg(company_domain) AS domains -- Collect all domains as an array
	FROM
		numbered_data
	GROUP BY
		updated_domain, source_name;
		
-- JSON creation with source_name as key and unclean domain array as value.
INSERT INTO da_new.company_master ( company_domain, source_table ) 
SELECT
	updated_domain,
	jsonb_object_agg(source_name, domains) AS source_table -- Create JSON object with source_name as key and domain array as value
FROM
	grouped_data
GROUP BY
	updated_domain;


-- Ensure no conflict with existing temporary or permanent table
DROP TABLE IF EXISTS source_wise_data;

-- Create a temporary table to store filtered data
CREATE TEMP TABLE source_wise_data AS
SELECT a.*
FROM grouped_data a
INNER JOIN test_source_table_223 b ON a.source_name = b.source_table;

-- Index the temporary table to improve join performance
CREATE INDEX ix_source_wise_data_source_name ON source_wise_data USING btree(source_name);

-- Insert data into the main table from a complex join involving a configuration table
INSERT INTO test_source_table_223 (source_table, domain_text, updated_text, column_name)
SELECT a.source_name, a.domains, a.updated_domain, b."column_name"
FROM source_wise_data a
INNER JOIN test_tbl_source_mapping_master b ON a.source_name = b.table_name AND b.company_master = 'company_domain';