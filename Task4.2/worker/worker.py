import asyncio
import os
import logging
from pyzeebe import ZeebeWorker, create_insecure_channel, Job, JobController

# Логирование
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ZEEBE_GATEWAY = os.getenv("ZEEBE_ADDRESS", "zeebe:26500")

# ----------------------------------------------------------------------
# Обработчики
# ----------------------------------------------------------------------

async def on_buiseness_error(exception: Exception, job: Job, job_controller: JobController) -> None:
    """
    on_error will be called when a task fails with an exception.
    We can use the job_controller to set the status.
    """
    print(f"Handling exception: {exception}")
    # Set the job status to error, which can be caught in the BPMN diagram
    await job_controller.set_error_status(f"Failed to handle job {job.key}. Error: {str(exception)}", str(exception))

async def handle_debit_money(job: Job):
    """Списание денег со счёта пользователя (тип: debit-money)"""
    logger.info(f"Выполняется задача: {job.type} (ID: {job.element_id})")
    amount = job.variables.get("amount", 0)

    # Имитация проверки баланса
    if amount > 10000:
        # Генерируем ошибку – активирует boundary event
        raise Exception("NOT_ENOUGH_MONEY")
    if amount < 0:
        # Другая ошибка (например, техническая) – активирует другой boundary event
        raise Exception("DEBIT_ERROR")

    # Успешное списание – сохраняем идентификатор транзакции
    return {"debit_transaction_id": "txn_12345"}

async def handle_process_antifrod(job: Job):
    """Проверка антифрод (тип: process-antifrod)"""
    logger.info(f"Выполняется задача: {job.type} (ID: {job.element_id})")
    risk_score = job.variables.get("risk_score", 0)

    if risk_score > 80:
        status = "MANUAL_REQUESTED"
    elif risk_score > 50:
        status = "REJECTED"
    else:
        status = "APPROVED"

    return {"applicationStatus": status}

async def handle_block_payment(job: Job):
    """Блокировка платежа (тип: block-payment)"""
    logger.info(f"Выполняется задача: {job.type} (ID: {job.element_id})")
    return {"blocked": True, "block_reason": "antifraud"}

async def handle_contractor_transfer(job: Job):
    """Перевод денег контрагенту (тип: contractor)"""
    logger.info(f"Выполняется задача: {job.type} (ID: {job.element_id})")
    fail = job.variables.get("fail_transfer", False)

    if fail:
        # Генерируем ошибку перевода – активирует boundary event
        raise Exception("TRANSFER_FAILED")

    return {"transfer_status": "COMPLETED"}

async def handle_refund_money(job: Job):
    """Возврат денег пользователю (тип: refund-money) – используется в двух местах"""
    logger.info(f"Выполняется задача: {job.type} (ID: {job.element_id})")
    return {"refunded": True, "refund_amount": job.variables.get("amount")}

async def handle_notify_debit_error(job: Job):
    """Уведомление об ошибке списания (тип: notify-debit-error)"""
    logger.info(f"Выполняется задача: {job.type} (ID: {job.element_id}) – отправка уведомления об ошибке списания")
    return

async def handle_notify_not_enough_money(job: Job):
    """Уведомление о недостатке средств (тип: notify-not-enough-money)"""
    logger.info(f"Выполняется задача: {job.type} (ID: {job.element_id}) – отправка уведомления о недостатке средств")
    return

async def handle_notify_payment_succeed(job: Job):
    """Уведомление об успешном платеже (тип: notify-payment-succeed)"""
    logger.info(f"Выполняется задача: {job.type} (ID: {job.element_id}) – платеж успешно завершён")
    return

async def handle_notify_payment_rejected(job: Job):
    """Уведомление об отклонении платежа (тип: notify-payment-rejected)"""
    logger.info(f"Выполняется задача: {job.type} (ID: {job.element_id}) – платеж отклонён")
    return

async def handle_notify_transfer_error(job: Job):
    """Уведомление об ошибке зачисления контрагенту (тип: notify-transfer-error)"""
    logger.info(f"Выполняется задача: {job.type} (ID: {job.element_id}) – ошибка при переводе контрагенту")
    return

async def main():
    channel = create_insecure_channel(ZEEBE_GATEWAY)
    worker = ZeebeWorker(channel)

    # Регистрируем задачи вручную
    worker.task(task_type="debit-money", exception_handler=on_buiseness_error)(handle_debit_money)
    worker.task(task_type="process-antifrod")(handle_process_antifrod)
    worker.task(task_type="block-payment")(handle_block_payment)
    worker.task(task_type="contractor", exception_handler=on_buiseness_error)(handle_contractor_transfer)
    worker.task(task_type="refund-money")(handle_refund_money)
    worker.task(task_type="notify-debit-error")(handle_notify_debit_error)
    worker.task(task_type="notify-not-enough-money")(handle_notify_not_enough_money)
    worker.task(task_type="notify-payment-succeed")(handle_notify_payment_succeed)
    worker.task(task_type="notify-payment-rejected")(handle_notify_payment_rejected)
    worker.task(task_type="notify-transfer-error")(handle_notify_transfer_error)

    logger.info("Воркер запущен, ожидание задач...")
    await worker.work()

if __name__ == "__main__":
    asyncio.run(main())