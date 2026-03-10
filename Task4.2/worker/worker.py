import asyncio
import os
import logging
from pyzeebe import ZeebeWorker, create_insecure_channel, JobController

# Логирование
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ZEEBE_GATEWAY = os.getenv("ZEEBE_ADDRESS", "zeebe:26500")

# Для локальной разработки приемлемо использовать insecure без TLS
channel = create_insecure_channel(ZEEBE_GATEWAY)
worker = ZeebeWorker(channel)

# ----------------------------------------------------------------------
# Обработчики
# ----------------------------------------------------------------------
@worker.task(task_type="debit-money")
async def handle_debit_money(job: JobController):
    """Списание денег со счёта пользователя (тип: debit-money)"""
    logger.info(f"Выполняется задача: {job.element_name} (ID: {job.element_id})")
    amount = job.variables.get("amount", 0)

    # Имитация проверки баланса
    if amount > 10000:
        # Генерируем ошибку – активирует boundary event
        await job.raise_error("NOT_ENOUGH_MONEY", "На счету недостаточно средств")
        return
    if amount < 0:
        # Другая ошибка (например, техническая) – активирует другой boundary event
        await job.raise_error("DEBIT_ERROR", "Ошибка при списании")
        return

    # Успешное списание – сохраняем идентификатор транзакции
    await job.set_variables({"debit_transaction_id": "txn_12345"}).complete()

@worker.task(task_type="process-antifrod")
async def handle_process_antifrod(job: JobController):
    """Проверка антифрод (тип: process-antifrod)"""
    logger.info(f"Выполняется задача: {job.element_name} (ID: {job.element_id})")
    risk_score = job.variables.get("risk_score", 0)

    if risk_score > 80:
        status = "MANUAL_REQUESTED"
    elif risk_score > 50:
        status = "REJECTED"
    else:
        status = "APPROVED"

    await job.set_variables({"applicationStatus": status}).complete()

@worker.task(task_type="block-payment")
async def handle_block_payment(job: JobController):
    """Блокировка платежа (тип: block-payment)"""
    logger.info(f"Выполняется задача: {job.element_name} (ID: {job.element_id})")
    await job.set_variables({"blocked": True, "block_reason": "antifraud"}).complete()

@worker.task(task_type="contractor")
async def handle_contractor_transfer(job: JobController):
    """Перевод денег контрагенту (тип: contractor)"""
    logger.info(f"Выполняется задача: {job.element_name} (ID: {job.element_id})")
    fail = job.variables.get("fail_transfer", False)

    if fail:
        # Генерируем ошибку перевода – активирует boundary event
        await job.raise_error("TRANSFER_FAILED", "Ошибка при переводе контрагенту")
        return

    await job.set_variables({"transfer_status": "COMPLETED"}).complete()

@worker.task(task_type="refund-money")
async def handle_refund_money(job: JobController):
    """Возврат денег пользователю (тип: refund-money) – используется в двух местах"""
    logger.info(f"Выполняется задача: {job.element_name} (ID: {job.element_id})")
    await job.set_variables({"refunded": True, "refund_amount": job.variables.get("amount")}).complete()

@worker.task(task_type="notify-debit-error")
async def handle_notify_debit_error(job: JobController):
    """Уведомление об ошибке списания (тип: notify-debit-error)"""
    logger.info(f"Выполняется задача: {job.element_name} (ID: {job.element_id}) – отправка уведомления об ошибке списания")
    await job.complete()

@worker.task(task_type="notify-not-enough-money")
async def handle_notify_not_enough_money(job: JobController):
    """Уведомление о недостатке средств (тип: notify-not-enough-money)"""
    logger.info(f"Выполняется задача: {job.element_name} (ID: {job.element_id}) – отправка уведомления о недостатке средств")
    await job.complete()

@worker.task(task_type="notify-payment-succeed")
async def handle_notify_payment_succeed(job: JobController):
    """Уведомление об успешном платеже (тип: notify-payment-succeed)"""
    logger.info(f"Выполняется задача: {job.element_name} (ID: {job.element_id}) – платеж успешно завершён")
    await job.complete()

@worker.task(task_type="notify-payment-rejected")
async def handle_notify_payment_rejected(job: JobController):
    """Уведомление об отклонении платежа (тип: notify-payment-rejected)"""
    logger.info(f"Выполняется задача: {job.element_name} (ID: {job.element_id}) – платеж отклонён")
    await job.complete()

@worker.task(task_type="notify-transfer-error")
async def handle_notify_transfer_error(job: JobController):
    """Уведомление об ошибке зачисления контрагенту (тип: notify-transfer-error)"""
    logger.info(f"Выполняется задача: {job.element_name} (ID: {job.element_id}) – ошибка при переводе контрагенту")
    await job.complete()

async def main():
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())