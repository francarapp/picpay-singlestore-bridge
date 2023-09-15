from pyspark import SparkContext
from pyspark.streaming import StreamingContext

from core import session
from bridge import BridgeUnionTransactions, BridgeInnerEvents
from conf import initlog

from datetime import datetime, timedelta

def conf():
    initlog()
    

def main():
    stream = BridgeInnerEvents("track", "event_transaction", evgroup="transaction", events=[
        "transaction_accomplished",
        "transaction_delayed_approved",
        "transaction_details_viewed",
        "transaction_error_raised",
        "transaction_expired",
        "transaction_fraud_check_analyzed",
        "transaction_gate_closed",
        "transaction_invoiced",
        "transaction_movement_canceled",
        "transaction_movement_confirmed",
        "transaction_movement_created",
        "transaction_movement_failed",
        "transaction_movement_not_found",
        "transaction_movement_requested",
        "transaction_product_enriched",
        "transaction_product_refused",
        "transaction_receipt_rollout",
        "transaction_redrived",
        "transaction_refund_enriched",
        "transaction_refund_errored",
        "transaction_refund_movement_canceled",
        "transaction_refund_movement_confirmed",
        "transaction_refund_movement_created",
        "transaction_refund_status_changed",
        "transaction_rollout",
        "transaction_saga_executed",
        "transaction_saga_failed",
        "transaction_saga_retry",
        "transaction_schedule_canceled",
        "transaction_schedule_created",
        "transaction_schedule_executed",
        "transaction_schedule_failed",
        "transaction_schedule_requested",
        "transaction_schedule_validated",
        "transaction_scheduled",
        "transaction_screen",
        "transaction_sherlock_redrive_exceeded",
        "transaction_sherlock_redrove",
        "transaction_started",
        "transaction_status_changed_error",
        "transaction_status_changed",
        "transaction_watson_executed"
    ], console=False)
    stream = stream.start()
    stream.awaitTermination()


if __name__ == "__main__":
    conf()
    main()
