-- =============================================================================
-- Seed data for testing .rsql scripts.
--
-- Provides a representative dataset that exercises the SQL logic in the
-- sample .rsql files: joins, filters, aggregations, NULL handling, etc.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Source individuals
-- ---------------------------------------------------------------------------
INSERT INTO source_data.individual
    (uri, customer_id, given_name_one, family_name, date_of_birth, gender, marital_status, citizenship, deceased_date, last_updated_date, last_updated_user)
VALUES
    ('uri/individual/001', 'CUST001', 'John',    'Smith',    '1985-03-15', 'Male',   'Married',  'GB', NULL,          '2024-01-10 10:00:00', 'system'),
    ('uri/individual/002', 'CUST002', 'Jane',    'Doe',      '1990-07-22', 'Female', 'Single',   'GB', NULL,          '2024-01-11 11:00:00', 'system'),
    ('uri/individual/003', 'CUST003', 'Robert',  'Johnson',  '1978-11-03', 'Male',   'Divorced', 'US', '2023-06-15', '2024-01-12 12:00:00', 'admin'),
    ('uri/individual/004', 'CUST004', 'Emily',   'Williams', '1995-01-30', 'Female', 'Married',  'GB', NULL,          '2024-01-13 09:00:00', 'system'),
    ('uri/individual/005', 'CUST005', 'Michael', 'Brown',    '1982-09-18', 'Male',   'Single',   'IE', NULL,          '2024-01-14 14:00:00', 'batch');

-- ---------------------------------------------------------------------------
-- Contacts
-- ---------------------------------------------------------------------------
INSERT INTO source_data.contact
    (customer_id, email_address, phone_number, phone_type, preferred_flag)
VALUES
    ('CUST001', 'john.smith@example.com',    '+44 7700 900001', 'Mobile', TRUE),
    ('CUST001', 'j.smith@work.example.com',  '+44 20 7946 0001', 'Work', FALSE),
    ('CUST002', 'jane.doe@example.com',      '+44 7700 900002', 'Mobile', TRUE),
    ('CUST003', 'r.johnson@example.com',     '+44 7700 900003', 'Mobile', TRUE),
    ('CUST004', 'emily.w@example.com',       '+44 7700 900004', 'Mobile', TRUE),
    ('CUST005', NULL,                         '+44 7700 900005', 'Mobile', TRUE);

-- ---------------------------------------------------------------------------
-- Locations
-- ---------------------------------------------------------------------------
INSERT INTO source_data.location
    (uri, customer_id, address_line_one, address_line_two, city, county, postal_code, country, address_type)
VALUES
    ('uri/location/001', 'CUST001', '10 Downing Street',  NULL,           'London',     'Greater London', 'SW1A 2AA', 'GB', 'Home'),
    ('uri/location/002', 'CUST001', '1 Office Park',      'Building A',   'Manchester', 'Greater Manchester', 'M1 1AA', 'GB', 'Work'),
    ('uri/location/003', 'CUST002', '25 High Street',     'Flat 3B',      'Edinburgh',  'Midlothian',    'EH1 1AA', 'GB', 'Home'),
    ('uri/location/004', 'CUST003', '100 Main Road',      NULL,           'Dublin',     'Dublin',        'D01 F5P2', 'IE', 'Home'),
    ('uri/location/005', 'CUST004', '5 Park Avenue',      'Suite 200',    'Bristol',    'Avon',          'BS1 1AA', 'GB', 'Home'),
    ('uri/location/006', 'CUST005', '42 Oak Lane',        NULL,           'Cardiff',    'South Glamorgan','CF10 1AA', 'GB', 'Home');

-- ---------------------------------------------------------------------------
-- Contract summaries
-- ---------------------------------------------------------------------------
INSERT INTO source_data.contract_summary
    (uri, customer_id, policy_id, policy_type, status, start_date, end_date, premium_amount)
VALUES
    ('uri/contract/001', 'CUST001', 'POL001', 'Life',     'Active',    '2020-01-01', '2050-01-01', 150.00),
    ('uri/contract/002', 'CUST001', 'POL002', 'Home',     'Active',    '2023-06-01', '2024-06-01', 45.50),
    ('uri/contract/003', 'CUST002', 'POL003', 'Motor',    'Active',    '2023-09-01', '2024-09-01', 85.00),
    ('uri/contract/004', 'CUST003', 'POL004', 'Life',     'Lapsed',    '2018-01-01', '2023-01-01', 200.00),
    ('uri/contract/005', 'CUST003', 'POL005', 'Pension',  'Active',    '2015-03-15', '2045-03-15', 500.00),
    ('uri/contract/006', 'CUST004', 'POL006', 'Health',   'Active',    '2024-01-01', '2025-01-01', 120.00),
    ('uri/contract/007', 'CUST005', 'POL007', 'Motor',    'Cancelled', '2022-01-01', '2023-01-01', 95.00);

-- ---------------------------------------------------------------------------
-- Reference data
-- ---------------------------------------------------------------------------
INSERT INTO source_data.reference_data
    (lookup_type, lookup_code, lookup_value, source_system, active_flag)
VALUES
    ('GENDER',         'M',    'Male',      'MDM',    TRUE),
    ('GENDER',         'F',    'Female',    'MDM',    TRUE),
    ('GENDER',         'M',    'Male',      'RELTIO', TRUE),
    ('GENDER',         'F',    'Female',    'RELTIO', TRUE),
    ('MARITAL_STATUS', 'MAR',  'Married',   'MDM',    TRUE),
    ('MARITAL_STATUS', 'SNG',  'Single',    'MDM',    TRUE),
    ('MARITAL_STATUS', 'DIV',  'Divorced',  'MDM',    TRUE),
    ('MARITAL_STATUS', 'MAR',  'Married',   'RELTIO', TRUE),
    ('MARITAL_STATUS', 'SNG',  'Single',    'RELTIO', TRUE),
    ('MARITAL_STATUS', 'SEP',  'Separated', 'RELTIO', FALSE),
    ('POLICY_TYPE',    'LIFE', 'Life',      'MDM',    TRUE),
    ('POLICY_TYPE',    'HOME', 'Home',      'MDM',    TRUE),
    ('POLICY_TYPE',    'MTR',  'Motor',     'MDM',    TRUE);
