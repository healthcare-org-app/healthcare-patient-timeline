"""Kafka consumers for patient-timeline-service.

One handler per subscribed topic. Handlers are best-effort logging plus
audit — services override this file to implement real cross-domain behavior.
"""
from __future__ import annotations

import logging

from healthcare_common.audit import emit_audit

log = logging.getLogger("patient-timeline-service.consumers")


def register(svc) -> None:
    bus = svc.bus

    @bus.on("patient.created")
    def _on_patient_created(envelope: dict) -> None:
        log.info("patient-timeline-service: received patient.created id=%s", envelope.get("id"))
        emit_audit(bus, action="consume.patient.created", actor="system:patient-timeline-service",
                   target=None, details={"envelope_id": envelope.get("id")})

    @bus.on("patient.updated")
    def _on_patient_updated(envelope: dict) -> None:
        log.info("patient-timeline-service: received patient.updated id=%s", envelope.get("id"))
        emit_audit(bus, action="consume.patient.updated", actor="system:patient-timeline-service",
                   target=None, details={"envelope_id": envelope.get("id")})

    @bus.on("patient.merged")
    def _on_patient_merged(envelope: dict) -> None:
        log.info("patient-timeline-service: received patient.merged id=%s", envelope.get("id"))
        emit_audit(bus, action="consume.patient.merged", actor="system:patient-timeline-service",
                   target=None, details={"envelope_id": envelope.get("id")})

    @bus.on("encounter.started")
    def _on_encounter_started(envelope: dict) -> None:
        log.info("patient-timeline-service: received encounter.started id=%s", envelope.get("id"))
        emit_audit(bus, action="consume.encounter.started", actor="system:patient-timeline-service",
                   target=None, details={"envelope_id": envelope.get("id")})

    @bus.on("lab.result.available")
    def _on_lab_result_available(envelope: dict) -> None:
        log.info("patient-timeline-service: received lab.result.available id=%s", envelope.get("id"))
        emit_audit(bus, action="consume.lab.result.available", actor="system:patient-timeline-service",
                   target=None, details={"envelope_id": envelope.get("id")})

    @bus.on("imaging.result.available")
    def _on_imaging_result_available(envelope: dict) -> None:
        log.info("patient-timeline-service: received imaging.result.available id=%s", envelope.get("id"))
        emit_audit(bus, action="consume.imaging.result.available", actor="system:patient-timeline-service",
                   target=None, details={"envelope_id": envelope.get("id")})

    @bus.on("prescription.issued")
    def _on_prescription_issued(envelope: dict) -> None:
        log.info("patient-timeline-service: received prescription.issued id=%s", envelope.get("id"))
        emit_audit(bus, action="consume.prescription.issued", actor="system:patient-timeline-service",
                   target=None, details={"envelope_id": envelope.get("id")})

    @bus.on("appointment.booked")
    def _on_appointment_booked(envelope: dict) -> None:
        log.info("patient-timeline-service: received appointment.booked id=%s", envelope.get("id"))
        emit_audit(bus, action="consume.appointment.booked", actor="system:patient-timeline-service",
                   target=None, details={"envelope_id": envelope.get("id")})

    @bus.on("invoice.issued")
    def _on_invoice_issued(envelope: dict) -> None:
        log.info("patient-timeline-service: received invoice.issued id=%s", envelope.get("id"))
        emit_audit(bus, action="consume.invoice.issued", actor="system:patient-timeline-service",
                   target=None, details={"envelope_id": envelope.get("id")})

