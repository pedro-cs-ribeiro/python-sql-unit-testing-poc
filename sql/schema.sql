-- =============================================================================
-- Schema DDL for testing .rsql scripts against PostgreSQL.
--
-- This schema mirrors the Redshift table structures used in production
-- pipelines, but with Redshift-specific syntax removed (DISTKEY, SORTKEY,
-- ENCODE, etc.) so it runs on plain PostgreSQL.
--
-- In a real project, this file would be derived from the production DDL files
-- (e.g., Entities_DDL.sql) by running them through the compatibility layer.
-- =============================================================================

-- Schema: source data (equivalent to production staging/input schemas)
CREATE SCHEMA IF NOT EXISTS source_data;

-- Schema: target data (equivalent to production outbound/work schemas)
CREATE SCHEMA IF NOT EXISTS target_data;

-- Schema: reconciliation (equivalent to production recon schemas)
CREATE SCHEMA IF NOT EXISTS recon_data;

-- =============================================================================
-- Source tables (simulate data coming from upstream systems)
-- =============================================================================

CREATE TABLE source_data.individual (
    uri                VARCHAR(255),
    customer_id        VARCHAR(50) PRIMARY KEY,
    given_name_one     VARCHAR(100),
    family_name        VARCHAR(100),
    date_of_birth      DATE,
    gender             VARCHAR(20),
    marital_status     VARCHAR(50),
    citizenship        VARCHAR(50),
    deceased_date      DATE,
    last_updated_date  TIMESTAMP,
    last_updated_user  VARCHAR(100),
    scm_createdtime    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    scm_updatedtime    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE source_data.contact (
    customer_id        VARCHAR(50),
    email_address      VARCHAR(255),
    phone_number       VARCHAR(50),
    phone_type         VARCHAR(20),
    preferred_flag     BOOLEAN DEFAULT FALSE,
    scm_createdtime    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE source_data.location (
    uri                VARCHAR(255),
    customer_id        VARCHAR(50),
    address_line_one   VARCHAR(255),
    address_line_two   VARCHAR(255),
    city               VARCHAR(100),
    county             VARCHAR(100),
    postal_code        VARCHAR(20),
    country            VARCHAR(50),
    address_type       VARCHAR(50),
    scm_createdtime    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    scm_updatedtime    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE source_data.contract_summary (
    uri                VARCHAR(255),
    customer_id        VARCHAR(50),
    policy_id          VARCHAR(50),
    policy_type        VARCHAR(50),
    status             VARCHAR(20),
    start_date         DATE,
    end_date           DATE,
    premium_amount     DECIMAL(12, 2),
    scm_createdtime    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    scm_updatedtime    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE source_data.reference_data (
    lookup_type        VARCHAR(100),
    lookup_code        VARCHAR(100),
    lookup_value       VARCHAR(255),
    source_system      VARCHAR(50),
    active_flag        BOOLEAN DEFAULT TRUE
);

-- =============================================================================
-- Target/work tables (populated by .rsql scripts)
-- =============================================================================

CREATE TABLE target_data.individual (
    uri                VARCHAR(255),
    customer_id        VARCHAR(50) PRIMARY KEY,
    given_name_one     VARCHAR(100),
    family_name        VARCHAR(100),
    date_of_birth      DATE,
    gender             VARCHAR(20),
    marital_status     VARCHAR(50),
    citizenship        VARCHAR(50),
    deceased_date      DATE,
    last_updated_date  TIMESTAMP,
    last_updated_user  VARCHAR(100),
    scm_createdtime    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    scm_updatedtime    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE target_data.customer_mapping (
    mapping_id         SERIAL PRIMARY KEY,
    customer_id        VARCHAR(50),
    customer_uri       VARCHAR(255),
    created_date       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by         VARCHAR(50)
);

-- =============================================================================
-- Reconciliation tables (populated by recon .rsql scripts)
-- =============================================================================

-- This table is intentionally created empty; CTAS scripts will create
-- their own result tables dynamically.
