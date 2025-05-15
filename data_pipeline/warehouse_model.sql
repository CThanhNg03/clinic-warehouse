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

-- Upsert triggers for tables with unique keys

-- patient.dim_patient
CREATE OR REPLACE FUNCTION patient_dim_patient_upsert() RETURNS TRIGGER AS $$
BEGIN
    UPDATE patient.dim_patient
    SET
        gender = NEW.gender,
        birth_date = NEW.birth_date,
        deceased_date_time = NEW.deceased_date_time,
        multiple_birth = NEW.multiple_birth,
        marital_status = NEW.marital_status
    WHERE patient_key = NEW.patient_key;


    IF NOT FOUND THEN
        RETURN NEW; -- Allow insert to proceed
    ELSE
        RETURN NULL; -- Prevent insert (already updated)
    END IF;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_patient_dim_patient_upsert ON patient.dim_patient;

CREATE TRIGGER trg_patient_dim_patient_upsert
BEFORE INSERT ON patient.dim_patient
FOR EACH ROW
EXECUTE FUNCTION patient_dim_patient_upsert();

-- encounter.dim_encounter_type
CREATE OR REPLACE FUNCTION encounter_dim_encounter_type_upsert() RETURNS TRIGGER AS $$
BEGIN
    UPDATE encounter.dim_encounter_type
    SET
        encounter_type_name = NEW.encounter_type_name
    WHERE encounter_type_key = NEW.encounter_type_key;

    IF NOT FOUND THEN
        RETURN NEW; -- Allow insert to proceed
    ELSE
        RETURN NULL; -- Prevent insert (already updated)
    END IF;

END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_encounter_dim_encounter_type_upsert ON encounter.dim_encounter_type;

CREATE TRIGGER trg_encounter_dim_encounter_type_upsert
BEFORE INSERT ON encounter.dim_encounter_type
FOR EACH ROW
EXECUTE FUNCTION encounter_dim_encounter_type_upsert();

-- encounter.dim_encounter_reason
CREATE OR REPLACE FUNCTION encounter_dim_encounter_reason_upsert() RETURNS TRIGGER AS $$
BEGIN
    UPDATE encounter.dim_encounter_reason
    SET
        encounter_reason_name = NEW.encounter_reason_name
    WHERE encounter_reason_key = NEW.encounter_reason_key;

    IF NOT FOUND THEN
        RETURN NEW; -- Allow insert to proceed
    ELSE
        RETURN NULL; -- Prevent insert (already updated)
    END IF;

END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_encounter_dim_encounter_reason_upsert ON encounter.dim_encounter_reason;

CREATE TRIGGER trg_encounter_dim_encounter_reason_upsert
BEFORE INSERT ON encounter.dim_encounter_reason
FOR EACH ROW
EXECUTE FUNCTION encounter_dim_encounter_reason_upsert();

-- encounter.fact_encounter
CREATE OR REPLACE FUNCTION encounter_fact_encounter_upsert() RETURNS TRIGGER AS $$
BEGIN
    UPDATE encounter.fact_encounter
    SET
        patient_key = NEW.patient_key,
        encounter_status = NEW.encounter_status,
        encounter_class = NEW.encounter_class,
        encounter_type_key = NEW.encounter_type_key,
        encounter_reason_key = NEW.encounter_reason_key,
        period_start = NEW.period_start,
        period_end = NEW.period_end
    WHERE encounter_key = NEW.encounter_key;


    IF NOT FOUND THEN
        RETURN NEW; -- Allow insert to proceed
    ELSE
        RETURN NULL; -- Prevent insert (already updated)
    END IF;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_encounter_fact_encounter_upsert ON encounter.fact_encounter;

CREATE TRIGGER trg_encounter_fact_encounter_upsert
BEFORE INSERT ON encounter.fact_encounter
FOR EACH ROW
EXECUTE FUNCTION encounter_fact_encounter_upsert();

-- clinical.dim_vaccine_code
CREATE OR REPLACE FUNCTION clinical_dim_vaccine_code_upsert() RETURNS TRIGGER AS $$
BEGIN
    UPDATE clinical.dim_vaccine_code
    SET
        vaccine_name = NEW.vaccine_name
    WHERE vaccine_code_key = NEW.vaccine_code_key;

    IF NOT FOUND THEN
        RETURN NEW; -- Allow insert to proceed
    ELSE
        RETURN NULL; -- Prevent insert (already updated)
    END IF;

END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_clinical_dim_vaccine_code_upsert ON clinical.dim_vaccine_code;

CREATE TRIGGER trg_clinical_dim_vaccine_code_upsert
BEFORE INSERT ON clinical.dim_vaccine_code
FOR EACH ROW
EXECUTE FUNCTION clinical_dim_vaccine_code_upsert();

-- clinical.dim_procedure_type
CREATE OR REPLACE FUNCTION clinical_dim_procedure_type_upsert() RETURNS TRIGGER AS $$
BEGIN
    UPDATE clinical.dim_procedure_type
    SET
        procedure_type_name = NEW.procedure_type_name
    WHERE procedure_type_key = NEW.procedure_type_key;

    IF NOT FOUND THEN
        RETURN NEW; -- Allow insert to proceed
    ELSE
        RETURN NULL; -- Prevent insert (already updated)
    END IF;

END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_clinical_dim_procedure_type_upsert ON clinical.dim_procedure_type;

CREATE TRIGGER trg_clinical_dim_procedure_type_upsert
BEFORE INSERT ON clinical.dim_procedure_type
FOR EACH ROW
EXECUTE FUNCTION clinical_dim_procedure_type_upsert();

-- clinical.dim_observation_code
CREATE OR REPLACE FUNCTION clinical_dim_observation_code_upsert() RETURNS TRIGGER AS $$
BEGIN
    UPDATE clinical.dim_observation_code
    SET
        observation_name = NEW.observation_name
    WHERE observation_code_key = NEW.observation_code_key;

    IF NOT FOUND THEN
        RETURN NEW; -- Allow insert to proceed
    ELSE
        RETURN NULL; -- Prevent insert (already updated)
    END IF;

END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_clinical_dim_observation_code_upsert ON clinical.dim_observation_code;

CREATE TRIGGER trg_clinical_dim_observation_code_upsert
BEFORE INSERT ON clinical.dim_observation_code
FOR EACH ROW
EXECUTE FUNCTION clinical_dim_observation_code_upsert();

-- clinical.dim_condition_code
CREATE OR REPLACE FUNCTION clinical_dim_condition_code_upsert() RETURNS TRIGGER AS $$
BEGIN
    UPDATE clinical.dim_condition_code
    SET
        condition_name = NEW.condition_name
    WHERE condition_code_key = NEW.condition_code_key;

    IF NOT FOUND THEN
        RETURN NEW; -- Allow insert to proceed
    ELSE
        RETURN NULL; -- Prevent insert (already updated)
    END IF;

END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_clinical_dim_condition_code_upsert ON clinical.dim_condition_code;

CREATE TRIGGER trg_clinical_dim_condition_code_upsert
BEFORE INSERT ON clinical.dim_condition_code
FOR EACH ROW
EXECUTE FUNCTION clinical_dim_condition_code_upsert();

-- clinical.dim_medication
CREATE OR REPLACE FUNCTION clinical_dim_medication_upsert() RETURNS TRIGGER AS $$
BEGIN
    UPDATE clinical.dim_medication
    SET
        medication_name = NEW.medication_name
    WHERE medication_code_key = NEW.medication_code_key;
    IF NOT FOUND THEN
        RETURN NEW; -- Allow insert to proceed
    ELSE
        RETURN NULL; -- Prevent insert (already updated)
    END IF;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_clinical_dim_medication_upsert ON clinical.dim_medication;

CREATE TRIGGER trg_clinical_dim_medication_upsert
BEFORE INSERT ON clinical.dim_medication
FOR EACH ROW
EXECUTE FUNCTION clinical_dim_medication_upsert();

-- clinical.dim_diagnostic_code
CREATE OR REPLACE FUNCTION clinical_dim_diagnostic_code_upsert() RETURNS TRIGGER AS $$
BEGIN
    UPDATE clinical.dim_diagnostic_code
    SET
        diagnostic_name = NEW.diagnostic_name
    WHERE diagnostic_code_key = NEW.diagnostic_code_key;

    IF NOT FOUND THEN
        RETURN NEW; -- Allow insert to proceed
    ELSE
        RETURN NULL; -- Prevent insert (already updated)
    END IF;

END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_clinical_dim_diagnostic_code_upsert ON clinical.dim_diagnostic_code;

CREATE TRIGGER trg_clinical_dim_diagnostic_code_upsert
BEFORE INSERT ON clinical.dim_diagnostic_code
FOR EACH ROW
EXECUTE FUNCTION clinical_dim_diagnostic_code_upsert();

-- clinical.dim_allergy_code
CREATE OR REPLACE FUNCTION clinical_dim_allergy_code_upsert() RETURNS TRIGGER AS $$
BEGIN
    UPDATE clinical.dim_allergy_code
    SET
        allergy_name = NEW.allergy_name,
        type = NEW.type,
        category = NEW.category
    WHERE allergy_code_key = NEW.allergy_code_key;

    IF NOT FOUND THEN
        RETURN NEW; -- Allow insert to proceed
    ELSE
        RETURN NULL; -- Prevent insert (already updated)
    END IF;

END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_clinical_dim_allergy_code_upsert ON clinical.dim_allergy_code;

CREATE TRIGGER trg_clinical_dim_allergy_code_upsert
BEFORE INSERT ON clinical.dim_allergy_code
FOR EACH ROW
EXECUTE FUNCTION clinical_dim_allergy_code_upsert();

-- clinical.fact_condition
CREATE OR REPLACE FUNCTION clinical_fact_condition_upsert() RETURNS TRIGGER AS $$
BEGIN
    UPDATE clinical.fact_condition
    SET
        onset_date_time = NEW.onset_date_time,
        clinical_status = NEW.clinical_status,
        verification_status = NEW.verification_status,
        condition_code_key = NEW.condition_code_key,
        patient_key = NEW.patient_key,
        encounter_key = NEW.encounter_key,
        abatement_date_time = NEW.abatement_date_time
    WHERE condition_key = NEW.condition_key;    

    IF NOT FOUND THEN
        RETURN NEW; -- Allow insert to proceed
    ELSE
        RETURN NULL; -- Prevent insert (already updated)
    END IF;

END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_clinical_fact_condition_upsert ON clinical.fact_condition;

CREATE TRIGGER trg_clinical_fact_condition_upsert
BEFORE INSERT ON clinical.fact_condition
FOR EACH ROW
EXECUTE FUNCTION clinical_fact_condition_upsert();

-- clinical.fact_observation
CREATE OR REPLACE FUNCTION clinical_fact_observation_upsert() RETURNS TRIGGER AS $$
BEGIN
    UPDATE clinical.fact_observation
    SET
        observation_status = NEW.observation_status,
        issued = NEW.issued,
        effective_date_time = NEW.effective_date_time,
        patient_key = NEW.patient_key,
        encounter_key = NEW.encounter_key,
        observation_code_key = NEW.observation_code_key
    WHERE observation_key = NEW.observation_key;


    IF NOT FOUND THEN
        RETURN NEW; -- Allow insert to proceed
    ELSE
        RETURN NULL; -- Prevent insert (already updated)
    END IF;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_clinical_fact_observation_upsert ON clinical.fact_observation;

CREATE TRIGGER trg_clinical_fact_observation_upsert
BEFORE INSERT ON clinical.fact_observation
FOR EACH ROW
EXECUTE FUNCTION clinical_fact_observation_upsert();

-- clinical.fact_additional_instruction
CREATE OR REPLACE FUNCTION clinical_fact_additional_instruction_upsert() RETURNS TRIGGER AS $$
BEGIN
    UPDATE clinical.fact_additional_instruction
    SET
        additional_instruction_display = NEW.additional_instruction_display,
        additional_instruction_code = NEW.additional_instruction_code
    WHERE medication_request_key = NEW.medication_request_key;
    

    IF NOT FOUND THEN
        RETURN NEW; -- Allow insert to proceed
    ELSE
        RETURN NULL; -- Prevent insert (already updated)
    END IF;

END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_clinical_fact_additional_instruction_upsert ON clinical.fact_additional_instruction;

CREATE TRIGGER trg_clinical_fact_additional_instruction_upsert
BEFORE INSERT ON clinical.fact_additional_instruction
FOR EACH ROW
EXECUTE FUNCTION clinical_fact_additional_instruction_upsert();

-- clinical.fact_diagnostic_report
CREATE OR REPLACE FUNCTION clinical_fact_diagnostic_report_upsert() RETURNS TRIGGER AS $$
BEGIN
    UPDATE clinical.fact_diagnostic_report
    SET
        diagnostic_status = NEW.diagnostic_status,
        issued = NEW.issued,
        effective_date_time = NEW.effective_date_time,
        patient_key = NEW.patient_key,
        encounter_key = NEW.encounter_key,
        diagnostic_code_key = NEW.diagnostic_code_key
    WHERE diagnostic_key = NEW.diagnostic_key;
    

    IF NOT FOUND THEN
        RETURN NEW; -- Allow insert to proceed
    ELSE
        RETURN NULL; -- Prevent insert (already updated)
    END IF;

END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_clinical_fact_diagnostic_report_upsert ON clinical.fact_diagnostic_report;

CREATE TRIGGER trg_clinical_fact_diagnostic_report_upsert
BEFORE INSERT ON clinical.fact_diagnostic_report
FOR EACH ROW
EXECUTE FUNCTION clinical_fact_diagnostic_report_upsert();

-- care.dim_careplan_category
CREATE OR REPLACE FUNCTION care_dim_careplan_category_upsert() RETURNS TRIGGER AS $$
BEGIN
    UPDATE care.dim_careplan_category
    SET
        careplan_category_name = NEW.careplan_category_name
    WHERE careplan_category_key = NEW.careplan_category_key;

    IF NOT FOUND THEN
        RETURN NEW; -- Allow insert to proceed
    ELSE
        RETURN NULL; -- Prevent insert (already updated)
    END IF;

END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_care_dim_careplan_category_upsert ON care.dim_careplan_category;

CREATE TRIGGER trg_care_dim_careplan_category_upsert
BEFORE INSERT ON care.dim_careplan_category
FOR EACH ROW
EXECUTE FUNCTION care_dim_careplan_category_upsert();

-- care.dim_careplan_activity
CREATE OR REPLACE FUNCTION care_dim_careplan_activity_upsert() RETURNS TRIGGER AS $$
BEGIN
    UPDATE care.dim_careplan_activity
    SET
        careplan_activity_name = NEW.careplan_activity_name
    WHERE careplan_activity_key = NEW.careplan_activity_key;

    IF NOT FOUND THEN
        RETURN NEW; -- Allow insert to proceed
    ELSE
        RETURN NULL; -- Prevent insert (already updated)
    END IF;

END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_care_dim_careplan_activity_upsert ON care.dim_careplan_activity;

CREATE TRIGGER trg_care_dim_careplan_activity_upsert
BEFORE INSERT ON care.dim_careplan_activity
FOR EACH ROW
EXECUTE FUNCTION care_dim_careplan_activity_upsert();