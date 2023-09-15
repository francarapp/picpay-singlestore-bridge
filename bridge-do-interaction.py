from pyspark import SparkContext
from pyspark.streaming import StreamingContext

from core import session
from bridge import BridgeUnionTransactions, BridgeInnerEvents
from conf import initlog

from datetime import datetime, timedelta

def conf():
    initlog()
    

def main():
    stream = BridgeInnerEvents("track", "event_interaction", evgroup="interaction", events=[
        "button_clicked",
        "bottom_sheet_accessed",
        "bottom_sheet_item_clicked",
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
	    "dialog_option_select",
	    "dialog_option_selected",
	    "dialog_viewed",	
	    "feed_card_interacted",
	    "feed_page_loaded",	
	    "tab_clicked",
	    "tab_selected",
	    "tab_viewed"        
    ], console=False)
    stream = stream.start()
    stream.awaitTermination()


if __name__ == "__main__":
    conf()
    main()
