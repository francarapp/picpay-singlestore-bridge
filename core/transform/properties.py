from pyspark.sql.functions import map_filter, lit, col


def reshapeProperties(df, evname):
    match evname:
        case 'button_clicked', "bottom_sheet_accessed", "bottom_sheet_item_clicked", "bottom_sheet_accessed", \
                "bottom_sheet_item_clicked", "buttom_action_action_upgrade", "button_name", "button_selected",\
                "button_toggled", "button_viewed", "button":
            return withElementName(
                withProperties(df, ["button_name", "business_context", "screen_name", "provider"])
                    ,"button_name"
                )
        case "banner_clicked", "banner_viewed":
            return withElementName(
                withProperties(df, ["banner_name", "business_context", "screen_name"])
                    ,"banner_name"
                )
        case "card_clicked":
            return withElementName(
                withProperties(df, ["card_clicked", "card_interacted", "interaction_type", "business_context", "screen_name"])
                    ,"card_clicked"
                )
        case  "carousel_interacted", "carousel_viewed":
            return withElementName(
                withProperties(df, ["carrousel_name", "business_context", "screen_name"])
                    ,"carrousel_name"
                )
        case "dialog_clicked", "dialog_dismissed", "dialog_interacted", "dialog_option_selected", "dialog_viewed":
            return withElementName(
                withProperties(df, ["dialog_name", "business_context", "screen_name"])
                    ,"dialog_name"
                )
        case "dialog_option_selected":
            return withElementName(
                withProperties(df, ["dialog_name", "option_selected", "business_context", "screen_name"])
                    ,"dialog_name"
                )
        case "feed_card_interacted":
            return withElementName(
                withProperties(df, ["card_id", "tab_name", "card_type", "interaction_type", "business_context", "screen_name"])
                    ,"card_id"
                )
        case "feed_page_loaded":
            return withElementName(
                withProperties(df, ["tab_name", "pagination", "business_context", "screen_name"])
                    ,"tab_name"
                )
        case "filter_interacted":
            return withElementName(
                withProperties(df, ["business_context", "dialog_name", "filter_name", "interaction_type", "page_name", "user_id", "user_seller_id"])
                    ,"filter_name"
                )            
        case "push_notification_opened", "push_notification_received", "push_notification_showed":
            return withElementName(
                withProperties(df, ["notification_id", "push_type", "external_id", "external_orign", "notification_open_from", "campaign_id"])
                    ,"push_type"
                )
        case "search_cleared", "search_started", "search_focused", "search_option_selected", "search_returned":
            return withElementName(
                withProperties(df, ["business_context", "screen_name", "bussiness_id", "search_api_version", "is_search_result_list_empty"])
                    ,"screen_name"
                )
            
        case "tab_clicked", "tab_selected", "tab_viewed":
            return withElementName(
                withProperties(df, ["tab_name", "pagination", "business_context", "screen_name"])
                    ,"tab_name"
                )
            
        case other:
            return df



def withElementName(df, elname):
    return df.withColumn('interaction_element_name', col(elname))

def withProperties(df, columns):
    return df.withColumn("properties", map_filter(
        "properties", lambda k, v: k.isin(columns))
    )
