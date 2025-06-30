import pandas as pd

def validate_columns(df, required_columns):
    """Raise KeyError if any required columns are missing."""
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise KeyError(f"Missing columns in input data: {missing}")

def rename_columns(df, rename_map):
    """Rename columns using a mapping dictionary."""
    return df.rename(columns=rename_map)

def apply_value_mappings(df, form_config):
    """
    Apply value mappings from form_config['value_mappings'] to the DataFrame.
    """
    for col, mapping in form_config.get("value_mappings", {}).items():
        if col in df.columns:
            df[col] = df[col].map(mapping).fillna("")
    return df

def preprocess_dataframe(df, preprocess_funcs=None):
    """
    Apply a list of preprocessing functions to the DataFrame.
    Each function should take and return a DataFrame.
    """
    if preprocess_funcs:
        for func in preprocess_funcs:
            df = func(df)
    return df

def build_json_payloads(df, form_config, study_name, study_country, site):
    payloads = []
    unit_mappings = form_config.get("unit_mappings", {})
    item_links = form_config.get("item_links", {})

    subject_event_itemgroup_counter = {}

    for _, row in df.iterrows():
        subject = row.get("subject", row.get("Subject Number", ""))
        eventgroup_name = (
            form_config["eventgroup_logic"](row) if "eventgroup_logic" in form_config and callable(form_config["eventgroup_logic"])
            else form_config.get("eventgroup_name")
        )
        event_name = (
            form_config["event_logic"](row) if "event_logic" in form_config and callable(form_config["event_logic"])
            else form_config.get("event_name")
        )

        key = (subject, eventgroup_name, event_name)
        itemgroup_sequence = subject_event_itemgroup_counter.get(key, 1)
        subject_event_itemgroup_counter[key] = itemgroup_sequence + 1

        items = []
        for item_name, df_col in form_config["item_mappings"].items():
            is_link = (
                item_name in item_links and
                "item_to_form_link" in item_links[item_name]
            )
            value = row.get(df_col, "")
            if is_link:
                # For Item to Form Link, do NOT include "value"
                item = {
                     "item_name": item_name,
                    "value": "",
                     "item_to_form_link": item_links[item_name]["item_to_form_link"]
        }
                items.append(item)
            else:
                if value in [None, ""]:
                    continue  # skip empty values for normal items
                item = {"item_name": item_name, "value": value}
                if item_name in unit_mappings:
                    item["unit_value"] = unit_mappings[item_name]
                items.append(item)

        itemgroup = {
            "itemgroup_name": form_config["itemgroup_name"],
            "itemgroup_sequence": itemgroup_sequence,
            "items": items
        }
        form = {
            "study_country": study_country,
            "site": site,
            "subject": subject,
            "eventgroup_name": eventgroup_name,
            "eventgroup_sequence": 1,
            "event_name": event_name,
            "form_name": form_config["form_name"],
            "itemgroups": [itemgroup]
        }
        payload = {
            "study_name": study_name,
            "reopen": True,
            "submit": True,
            "change_reason": "Updated by the integration",
            "externally_owned": True,
            "form": form
        }
        payloads.append(payload)
    return payloads
def build_event_payloads(df, event_config, study_name, study_country, site):
    """
    Build event payloads for event API calls using a config dict.
    Supports both static and dynamic (callable) eventgroup_name and event_name.
    """
    events = []
    for _, row in df.iterrows():
        # Support dynamic (callable) or static eventgroup_name and event_name
        eventgroup_name = (
            event_config["eventgroup_name"](row)
            if callable(event_config.get("eventgroup_name"))
            else event_config.get("eventgroup_name")
        )
        event_name = (
            event_config["event_name"](row)
            if callable(event_config.get("event_name"))
            else event_config.get("event_name")
        )
        event = {
            "study_country": study_country,
            "site": site,
            "subject": row['subject'],
            "eventgroup_name": eventgroup_name,
            "eventgroup_sequence": event_config.get("eventgroup_sequence", 1),
            "event_name": event_name,
            "date": row[event_config["date_column"]],
            "change_reason": event_config.get("change_reason", "Action performed via the API"),
            "method": event_config.get("method", "on_site_visit__v"),
            "allow_planneddate_override": event_config.get("allow_planneddate_override", False),
            "externally_owned_date": event_config.get("externally_owned_date", True),
            "externally_owned_method": event_config.get("externally_owned_method", False)
        }
        events.append(event)
    return {
        "study_name": study_name,
        "events": events
    }