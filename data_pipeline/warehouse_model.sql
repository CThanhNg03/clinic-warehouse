-- Create schemas based on data meaning
DROP SCHEMA care CASCADE;
DROP SCHEMA clinical CASCADE;
DROP SCHEMA encounter CASCADE;
DROP SCHEMA patient CASCADE;


CREATE SCHEMA patient;
CREATE SCHEMA encounter;
CREATE SCHEMA clinical;
CREATE SCHEMA care;

-- Patient Schema
CREATE TABLE patient.dim_patient (
    patient_key VARCHAR PRIMARY KEY,
    gender VARCHAR(50),
    birth_date DATE,
    deceased_date_time TIMESTAMP,
    multiple_birth INT,
    marital_status VARCHAR(100)
);

CREATE TABLE patient.dim_patient_name (
    patient_key VARCHAR,
    name_use VARCHAR(50),
    last_name VARCHAR(100),
    first_name VARCHAR(100),
    prefix VARCHAR(50),
    suffix VARCHAR(50),
    FOREIGN KEY (patient_key) REFERENCES patient.dim_patient(patient_key)
);

CREATE TABLE patient.dim_patient_address (
    patient_key VARCHAR,
    latitude FLOAT,
    longitude FLOAT,
    city VARCHAR(100),
    state VARCHAR(100),
    postal_code VARCHAR(20),
    address_line TEXT,
    FOREIGN KEY (patient_key) REFERENCES patient.dim_patient(patient_key)
);

CREATE TABLE patient.dim_patient_telecom (
    patient_key VARCHAR,
    telecom_system VARCHAR(50),
    telecom_value TEXT,
    telecom_use VARCHAR(50),
    okay_to_leave_message BOOLEAN,
    FOREIGN KEY (patient_key) REFERENCES patient.dim_patient(patient_key)
);

CREATE TABLE patient.dim_patient_extension (
    patient_key VARCHAR,
    extension_type VARCHAR,
    extension_value TEXT,
    FOREIGN KEY (patient_key) REFERENCES patient.dim_patient(patient_key)
);

-- Encounter Schema
CREATE TABLE encounter.dim_encounter_type (
    encounter_type_key VARCHAR PRIMARY KEY,
    encounter_type_name VARCHAR(255)
);

CREATE TABLE encounter.dim_encounter_reason (
    encounter_reason_key VARCHAR PRIMARY KEY,
    encounter_reason_name VARCHAR(255)
);

CREATE TABLE encounter.fact_encounter (
    encounter_key VARCHAR PRIMARY KEY,
    patient_key VARCHAR,
    encounter_status VARCHAR(50),
    encounter_class VARCHAR(50),
    encounter_type_key VARCHAR,
    encounter_reason_key VARCHAR,
    period_start TIMESTAMP,
    period_end TIMESTAMP,
    FOREIGN KEY (patient_key) REFERENCES patient.dim_patient(patient_key),
    FOREIGN KEY (encounter_type_key) REFERENCES encounter.dim_encounter_type(encounter_type_key),
    FOREIGN KEY (encounter_reason_key) REFERENCES encounter.dim_encounter_reason(encounter_reason_key)
);

-- Clinical Schema
CREATE TABLE clinical.dim_vaccine_code (
    vaccine_code_key VARCHAR PRIMARY KEY,
    vaccine_name VARCHAR(255)
);

CREATE TABLE clinical.dim_procedure_type (
    procedure_type_key VARCHAR PRIMARY KEY,
    procedure_type_name VARCHAR(255)
);

CREATE TABLE clinical.dim_observation_code (
    observation_code_key VARCHAR PRIMARY KEY,
    observation_name VARCHAR(255) 
);

CREATE TABLE clinical.dim_condition_code (
    condition_code_key VARCHAR PRIMARY KEY,
    condition_name VARCHAR(255)
);

CREATE TABLE clinical.dim_medication (
    medication_code_key VARCHAR PRIMARY KEY,
    medication_name VARCHAR(255)
);

CREATE TABLE clinical.dim_diagnostic_code (
    diagnostic_code_key VARCHAR PRIMARY KEY,
    diagnostic_name VARCHAR(255)
);

CREATE TABLE clinical.dim_allergy_code (
    allergy_code_key VARCHAR PRIMARY KEY,
    allergy_name VARCHAR(255).
    type VARCHAR(50),
    category VARCHAR(50)
);

CREATE TABLE clinical.fact_condition (
    condition_key VARCHAR PRIMARY KEY,
    onset_date_time TIMESTAMP,
    clinical_status VARCHAR(50),
    verification_status VARCHAR(50),
    condition_code_key VARCHAR,
    patient_key VARCHAR,
    encounter_key VARCHAR,
    abatement_date_time TIMESTAMP,
    FOREIGN KEY (condition_code_key) REFERENCES clinical.dim_condition_code(condition_code_key),
    FOREIGN KEY (patient_key) REFERENCES patient.dim_patient(patient_key),
    FOREIGN KEY (encounter_key) REFERENCES encounter.fact_encounter(encounter_key)
);

CREATE TABLE clinical.fact_immunization (
    patient_key VARCHAR,
    encounter_key VARCHAR,
    vaccine_code_key VARCHAR,
    immunization_date TIMESTAMP,
    immunization_status VARCHAR(50),
    was_not_given BOOLEAN,
    primary_source BOOLEAN,
    FOREIGN KEY (patient_key) REFERENCES patient.dim_patient(patient_key),
    FOREIGN KEY (encounter_key) REFERENCES encounter.fact_encounter(encounter_key),
    FOREIGN KEY (vaccine_code_key) REFERENCES clinical.dim_vaccine_code(vaccine_code_key)
);

CREATE TABLE clinical.fact_procedure (
    patient_key VARCHAR,
    encounter_key VARCHAR,
    reason_reference TEXT,
    procedure_type_key VARCHAR,
    procedure_status VARCHAR(50),
    performed_period_start TIMESTAMP,
    performed_period_end TIMESTAMP,
    FOREIGN KEY (patient_key) REFERENCES patient.dim_patient(patient_key),
    FOREIGN KEY (encounter_key) REFERENCES encounter.fact_encounter(encounter_key),
    FOREIGN KEY (procedure_type_key) REFERENCES clinical.dim_procedure_type(procedure_type_key)
);

CREATE TABLE clinical.fact_observation (
    observation_key VARCHAR PRIMARY KEY,
    observation_status VARCHAR(50),
    issued TIMESTAMP,
    effective_date_time TIMESTAMP,
    patient_key VARCHAR,
    encounter_key VARCHAR,
    observation_code_key VARCHAR,
    FOREIGN KEY (patient_key) REFERENCES patient.dim_patient(patient_key),
    FOREIGN KEY (encounter_key) REFERENCES encounter.fact_encounter(encounter_key)
);

CREATE TABLE clinical.fact_observation_result (
    observation_key VARCHAR,
    observation_code_key VARCHAR,
    observation_value TEXT,
    observation_unit VARCHAR,
    FOREIGN KEY (observation_key) REFERENCES clinical.fact_observation(observation_key)
);

CREATE TABLE clinical.fact_medication_request (
    medication_request_key VARCHAR,
    patient_key VARCHAR,
    date_written TIMESTAMP,
    encounter_key VARCHAR,
    medication_code_key VARCHAR,
    medication_request_status VARCHAR(50),
    reason_reference TEXT,
    medication_class VARCHAR(50),
    FOREIGN KEY (patient_key) REFERENCES patient.dim_patient(patient_key),
    FOREIGN KEY (encounter_key) REFERENCES encounter.fact_encounter(encounter_key),
    FOREIGN KEY (medication_code_key) REFERENCES clinical.dim_medication(medication_code_key)
);

CREATE TABLE clinical.fact_dispense_request (
    medication_request_key VARCHAR,
    number_of_repeats_allowed INT,
    quantity_value FLOAT,
    quantity_unit VARCHAR(50),
    expected_supply_duration_value FLOAT,
    expected_supply_duration_unit VARCHAR(50)
);

CREATE TABLE clinical.fact_dosage_instruction (
    medication_request_key VARCHAR,
    sequence INT,
    as_needed_boolean BOOLEAN,
    frequency INT,
    period INT,
    period_unit VARCHAR(50),
    dose_quantity FLOAT,
    additional_instructions_code VARCHAR(255)
);

CREATE TABLE clinical.fact_additional_instruction (
    medication_request_key VARCHAR PRIMARY KEY,
    additional_instruction_display VARCHAR,
    additional_instruction_code VARCHAR(255)
);

CREATE TABLE clinical.fact_diagnostic_report (
    diagnostic_key VARCHAR PRIMARY KEY,
    diagnostic_status VARCHAR(50),
    issued TIMESTAMP,
    effective_date_time TIMESTAMP,
    patient_key VARCHAR,
    encounter_key VARCHAR,
    diagnostic_code_key VARCHAR,
    FOREIGN KEY (patient_key) REFERENCES patient.dim_patient(patient_key),
    FOREIGN KEY (encounter_key) REFERENCES encounter.fact_encounter(encounter_key)
);

CREATE TABLE clinical.fact_diagnostic_result (
    diagnostic_key VARCHAR,
    result_key VARCHAR,
    result_display VARCHAR
);

CREATE TABLE clinical.fact_allergy (
    patient_key VARCHAR,
    allergy_code_key VARCHAR,
    criticality VARCHAR(50),
    clinical_status VARCHAR(50),
    asserted_date TIMESTAMP,
    FOREIGN KEY (patient_key) REFERENCES patient.dim_patient(patient_key)
  );

-- Care Schema
CREATE TABLE care.dim_careplan_category (
    careplan_category_key VARCHAR PRIMARY KEY,
    careplan_category_name VARCHAR(255)
);

CREATE TABLE care.dim_careplan_activity (
    careplan_activity_key VARCHAR PRIMARY KEY,
    careplan_activity_name VARCHAR(255)
);

CREATE TABLE care.fact_careplan (
    patient_key VARCHAR,
    encounter_key VARCHAR,
    careplan_status VARCHAR(50),
    careplan_activity_status VARCHAR(50),
    period_start TIMESTAMP,
    period_end TIMESTAMP,
    address_key VARCHAR,
    careplan_activity_code_key VARCHAR,
    careplan_category_key VARCHAR,
    FOREIGN KEY (patient_key) REFERENCES patient.dim_patient(patient_key),
    FOREIGN KEY (encounter_key) REFERENCES encounter.fact_encounter(encounter_key)
);




