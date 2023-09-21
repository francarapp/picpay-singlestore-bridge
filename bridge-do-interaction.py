from pyspark import SparkContext
from pyspark.streaming import StreamingContext

from core import session
from bridge import BridgeInnerEvents
from conf import initlog

from datetime import datetime, timedelta

def conf():
    initlog()
    

def main():
    stream = BridgeInnerEvents("track", "event_interaction", evgroup="interaction", events=[
        "button_clicked",
        "bottom_sheet_accessed",
        "bottom_sheet_item_clicked",
        "buttom_action_action_upgrade",
        
        "button_name",
        "button_selected",
        "button_toggled",
        "button_viewed",
        "button",
        
	    "banner_clicked",
	    "banner_viewed",
     
	    "card_clicked",
	    "card_interacted",
     
	    "carousel_interacted",
	    "carousel_viewed",
	    "dialog_clicked",
	    "dialog_dismissed",
	    "dialog_interacted",
     
	    "dialog_option_selected",
	    "dialog_viewed",	
	    "feed_card_interacted",
	    "feed_page_loaded",	
	    "tab_clicked",
	    "tab_selected",
	    "tab_viewed",
     
     	"filter_interacted",
	    "first_screen_viewed",
	    "first_time_open",
	    "search_cleared",
	    "search_error_reload",
	    "search_event",
	    "search_executed_on_searchable",
	    "search_focused",
	    "search_initiated",
	    "search_option_selected",
	    "search_result_error",
	    "search_result_interacted",
	    "search_results_viewed",
	    "search_returned",
	    "search_started",
	    "push_alert_sent",
	    "push_notification_communication_opened",
	    "push_notification_dismiss",
	    "push_notification_dismissed",
	    "push_notification_opened",
	    "push_notification_received",
	    "push_notification_showed",
	    "push_alert_sent",
	    "user_signed_in",
	    "user_signed_out",
	    "user_signed_up" 
    ], console=False)
    stream = stream.start()
    stream.awaitTermination()


if __name__ == "__main__":
    conf()
    main()
