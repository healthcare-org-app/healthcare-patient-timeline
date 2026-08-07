"""Kafka consumers for patient-timeline-service.

One handler per subscribed topic. Real handlers write to this service's own
database and/or publish follow-up events; stub handlers just log + audit.
"""
from __future__ import annotations

import logging

from psycopg.types.json import Json

from healthcare_common.audit import emit_audit

log = logging.getLogger("patient-timeline-service.consumers")

TABLE = "patient_timeline"


def register(svc) -> None:
    bus = svc.bus
    db = svc.db
    clients = svc.clients

    @bus.on("patient.created")
    def _on_patient_created(envelope: dict) -> None:
        data = envelope.get("data") or {}
        try:
                    # Append event to this service's timeline table under the associated patient.
                    pid = data.get("patient_id") or data.get("id")
                    db.execute(f"INSERT INTO {TABLE} (data) VALUES (%s)",
                               (Json({"patient_id": pid,
                                      "event_type": envelope.get("event_type"),
                                      "occurred_at": envelope.get("occurred_at"),
                                      "producer": envelope.get("producer"),
                                      "summary": data}),))
        except Exception as e:
            log.exception("patient-timeline-service/patient.created handler failed: %s", e)
        emit_audit(bus, action="consume.patient.created", actor="system:patient-timeline-service",
                   target=None, details={"envelope_id": envelope.get("id")})

    @bus.on("patient.updated")
    def _on_patient_updated(envelope: dict) -> None:
        data = envelope.get("data") or {}
        try:
                    # Append event to this service's timeline table under the associated patient.
                    pid = data.get("patient_id") or data.get("id")
                    db.execute(f"INSERT INTO {TABLE} (data) VALUES (%s)",
                               (Json({"patient_id": pid,
                                      "event_type": envelope.get("event_type"),
                                      "occurred_at": envelope.get("occurred_at"),
                                      "producer": envelope.get("producer"),
                                      "summary": data}),))
        except Exception as e:
            log.exception("patient-timeline-service/patient.updated handler failed: %s", e)
        emit_audit(bus, action="consume.patient.updated", actor="system:patient-timeline-service",
                   target=None, details={"envelope_id": envelope.get("id")})

    @bus.on("patient.merged")
    def _on_patient_merged(envelope: dict) -> None:
        data = envelope.get("data") or {}
        try:
                    old_id = data.get("old_id") or data.get("from_id")
                    new_id = data.get("new_id") or data.get("to_id")
                    if not (old_id and new_id): return
                    n = db.execute(
                        f"UPDATE {TABLE} SET data = jsonb_set(data, '{{patient_id}}', to_jsonb(%s::text)), updated_at=now() "
                        f"WHERE data->>'patient_id' = %s",
                        (str(new_id), str(old_id)),
                    )
                    log.info("patient.merged: %d rows re-linked %s->%s", n, old_id, new_id)
        except Exception as e:
            log.exception("patient-timeline-service/patient.merged handler failed: %s", e)
        emit_audit(bus, action="consume.patient.merged", actor="system:patient-timeline-service",
                   target=None, details={"envelope_id": envelope.get("id")})

    @bus.on("encounter.started")
    def _on_encounter_started(envelope: dict) -> None:
        data = envelope.get("data") or {}
        try:
                    # Append event to this service's timeline table under the associated patient.
                    pid = data.get("patient_id") or data.get("id")
                    db.execute(f"INSERT INTO {TABLE} (data) VALUES (%s)",
                               (Json({"patient_id": pid,
                                      "event_type": envelope.get("event_type"),
                                      "occurred_at": envelope.get("occurred_at"),
                                      "producer": envelope.get("producer"),
                                      "summary": data}),))
        except Exception as e:
            log.exception("patient-timeline-service/encounter.started handler failed: %s", e)
        emit_audit(bus, action="consume.encounter.started", actor="system:patient-timeline-service",
                   target=None, details={"envelope_id": envelope.get("id")})

    @bus.on("lab.result.available")
    def _on_lab_result_available(envelope: dict) -> None:
        data = envelope.get("data") or {}
        try:
                    # Append event to this service's timeline table under the associated patient.
                    pid = data.get("patient_id") or data.get("id")
                    db.execute(f"INSERT INTO {TABLE} (data) VALUES (%s)",
                               (Json({"patient_id": pid,
                                      "event_type": envelope.get("event_type"),
                                      "occurred_at": envelope.get("occurred_at"),
                                      "producer": envelope.get("producer"),
                                      "summary": data}),))
        except Exception as e:
            log.exception("patient-timeline-service/lab.result.available handler failed: %s", e)
        emit_audit(bus, action="consume.lab.result.available", actor="system:patient-timeline-service",
                   target=None, details={"envelope_id": envelope.get("id")})

    @bus.on("imaging.result.available")
    def _on_imaging_result_available(envelope: dict) -> None:
        data = envelope.get("data") or {}
        try:
                    # Append event to this service's timeline table under the associated patient.
                    pid = data.get("patient_id") or data.get("id")
                    db.execute(f"INSERT INTO {TABLE} (data) VALUES (%s)",
                               (Json({"patient_id": pid,
                                      "event_type": envelope.get("event_type"),
                                      "occurred_at": envelope.get("occurred_at"),
                                      "producer": envelope.get("producer"),
                                      "summary": data}),))
        except Exception as e:
            log.exception("patient-timeline-service/imaging.result.available handler failed: %s", e)
        emit_audit(bus, action="consume.imaging.result.available", actor="system:patient-timeline-service",
                   target=None, details={"envelope_id": envelope.get("id")})

    @bus.on("prescription.issued")
    def _on_prescription_issued(envelope: dict) -> None:
        data = envelope.get("data") or {}
        try:
                    # Append event to this service's timeline table under the associated patient.
                    pid = data.get("patient_id") or data.get("id")
                    db.execute(f"INSERT INTO {TABLE} (data) VALUES (%s)",
                               (Json({"patient_id": pid,
                                      "event_type": envelope.get("event_type"),
                                      "occurred_at": envelope.get("occurred_at"),
                                      "producer": envelope.get("producer"),
                                      "summary": data}),))
        except Exception as e:
            log.exception("patient-timeline-service/prescription.issued handler failed: %s", e)
        emit_audit(bus, action="consume.prescription.issued", actor="system:patient-timeline-service",
                   target=None, details={"envelope_id": envelope.get("id")})

    @bus.on("appointment.booked")
    def _on_appointment_booked(envelope: dict) -> None:
        data = envelope.get("data") or {}
        try:
                    # Append event to this service's timeline table under the associated patient.
                    pid = data.get("patient_id") or data.get("id")
                    db.execute(f"INSERT INTO {TABLE} (data) VALUES (%s)",
                               (Json({"patient_id": pid,
                                      "event_type": envelope.get("event_type"),
                                      "occurred_at": envelope.get("occurred_at"),
                                      "producer": envelope.get("producer"),
                                      "summary": data}),))
        except Exception as e:
            log.exception("patient-timeline-service/appointment.booked handler failed: %s", e)
        emit_audit(bus, action="consume.appointment.booked", actor="system:patient-timeline-service",
                   target=None, details={"envelope_id": envelope.get("id")})

    @bus.on("invoice.issued")
    def _on_invoice_issued(envelope: dict) -> None:
        data = envelope.get("data") or {}
        try:
                    # Append event to this service's timeline table under the associated patient.
                    pid = data.get("patient_id") or data.get("id")
                    db.execute(f"INSERT INTO {TABLE} (data) VALUES (%s)",
                               (Json({"patient_id": pid,
                                      "event_type": envelope.get("event_type"),
                                      "occurred_at": envelope.get("occurred_at"),
                                      "producer": envelope.get("producer"),
                                      "summary": data}),))
        except Exception as e:
            log.exception("patient-timeline-service/invoice.issued handler failed: %s", e)
        emit_audit(bus, action="consume.invoice.issued", actor="system:patient-timeline-service",
                   target=None, details={"envelope_id": envelope.get("id")})

