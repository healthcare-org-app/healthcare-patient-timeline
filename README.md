# patient-timeline-service

patient-timeline-service — domain: patients

- **Port:** 8106
- **Language:** Python 3.11 + Flask
- **Database:** `patients` (Postgres, table `patient_timeline`)
- **Event bus:** Kafka

## API

| Method    | Path                       |
|-----------|----------------------------|
| GET       | `/api/patient_timeline/`          |
| POST      | `/api/patient_timeline/`          |
| GET       | `/api/patient_timeline/<id>`      |
| PUT/PATCH | `/api/patient_timeline/<id>`      |
| DELETE    | `/api/patient_timeline/<id>`      |
| GET       | `/health`                  |
| GET       | `/ready`                   |

## Events

**Publishes:** (none)
**Subscribes:** patient.created, patient.updated, patient.merged, encounter.started, lab.result.available, imaging.result.available, prescription.issued, appointment.booked, invoice.issued

## HTTP peer dependencies

- `patients-service`
- `ehr-service`
- `audit-log-service`

## Local dev

```bash
pip install -e ../../libs/py-healthcare-common
pip install -r requirements.txt
cp .env.example .env
(cd ../../infra && docker compose up -d postgres kafka kafka-init)
python -m app.main
```

## Tests

```bash
pytest
```
