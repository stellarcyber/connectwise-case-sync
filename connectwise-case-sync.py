#!/usr/bin/env python

'''
	version:		20251203.000
	description:	connectwise integration script used to create Manage Service Tickets

    20251201.000    forked branch for improved efficiency and updated syncs
                    support for syncing CW resolution status and notes

    20251203.000    added capability for syncing audit records
    20251204.000    added capability for syncing ticket owner to case assignee
    20251204.001    changed the sync of ticket resolution to support all ticket status changes
    20251205.000    improved efficiency of checking sync'd CW tickets
                    added forced update for ownership
                    improved tracking for stellar cases

'''

import argparse
import yaml
from ConnectWise import ConnectWise
import STELLAR_UTIL
from LOGGER_UTIL import logger_util
from time import time, sleep
import os, traceback
import json

parser = argparse.ArgumentParser()
parser.add_argument('-l', '--log-file', help='Write stdout to logfile', dest='logfile', default='')
parser.add_argument('-d', '--debug', help='Turn on debug/verbose logging', dest='verbose', action='store_true')
parser.add_argument("-c", "--config", help='use yaml config (default: cw-make-ticket.yaml)', dest='yaml_config',
                    default='config.yaml')
parser.add_argument('-p', '--persistent-volume',
                    help='Path to persistent volume that contains the in-sync database and checkpoint timestamp files. \
                     If empty, then current directory is used and if not prepended with "/", relative paths are assumed. \
                     (NOTE: db and checkpoint files are created automatically) ', dest='data_volume', default='')
args = parser.parse_args()
l = logger_util(args)

def get_env():
    env_config = {}
    cw_host = os.environ.get("CW_HOST", '')
    cw_company_id = os.environ.get("CW_COMPANY_ID", '')
    cw_private_key = os.getenv("CW_PRIVATE_KEY", '')
    cw_client_id = os.getenv("CW_CLIENT_ID", '')
    stellar_dp = os.getenv("STELLAR_DP", '')
    stellar_user = os.getenv("STELLAR_USER", '')
    stellar_api_key = os.getenv("STELLAR_API_KEY", '')
    stellar_rbac_user = int(os.getenv("STELLAR_RBAC_USER", 0))
    stellar_saas = int(os.getenv('STELLAR_SAAS', 0))
    webhook_ingest_url = os.getenv('WEBHOOK_INGEST_URL', '')
    webhook_ingest_key = os.getenv('WEBHOOK_INGEST_KEY', '')
    if (cw_host and cw_company_id and cw_private_key and cw_client_id and stellar_api_key):
        env_config['cw_host'] = cw_host
        env_config['cw_company_id'] = cw_company_id
        env_config['cw_private_key'] = cw_private_key
        env_config['cw_client_id'] = cw_client_id
        env_config['stellar_dp'] = stellar_dp
        env_config['stellar_user'] = stellar_user
        env_config['stellar_api_key'] = stellar_api_key
        env_config['stellar_new_rbac_user_auth'] = stellar_rbac_user
        env_config['stellar_saas'] = stellar_saas
        env_config['webhook_ingest_url'] = webhook_ingest_url
        env_config['webhook_ingest_key'] = webhook_ingest_key
    else:
        raise Exception("Missing environmental variables for API keys")
    return env_config


if __name__ == "__main__":

    try:
        with open(args.yaml_config, 'r') as config_file:
            config = yaml.safe_load(config_file)
            config.update(get_env())
            l.configure(config)

        STELLAR_CHECKPOINT_FILENAME = "stellar_checkpoint"
        CW_CHECKPOINT_FILENAME = "cw_checkpoint"
        POLL_INTERVAL = int(config.get('stellar_polling_interval', 5)) * 60

        ''' syncs '''
        CW_SYNC_STATUS = config.get('cw_sync_status', False)
        if CW_SYNC_STATUS:
            CW_SYNC_STATUS_MAP = config.get('cw_sync_status_map', {})
        CW_SYNC_OWNER = config.get('cw_sync_ticket_owner', False)
        CW_FORCE_OWNER_SYNC = config.get('cw_force_owner_sync', False)
        CW_SYNC_NOTES = config.get('cw_sync_notes', False)
        CW_SYNC_AUDIT_RECORDS = config.get('cw_sync_audit_records', False)
        if CW_SYNC_AUDIT_RECORDS:
            # disabling note sync as this would be redundant
            CW_SYNC_NOTES = False

        CW = ConnectWise(logger=l, config=config)
        SU = STELLAR_UTIL.STELLAR_UTIL(logger=l, config=config, optional_data_path=args.data_volume)
        LDB = STELLAR_UTIL.local_db(ticket_table_name='cw_tickets', optional_db_dir=args.data_volume)

        ''' testing goes here '''
        # test 1

        ''' main loop for processing tickets / cases '''
        while True:

            ts_start_of_loop = time()

            ''''''
            '''   get CW tickets since checkpoint and compare with DB to see if they are sync'd '''
            ''''''
            r = CW.test_connection()
            NEW_CHECKPOINT_TS = int(time() * 1000)
            CHECKPOINT_TS = round(int(SU.checkpoint_read(filepath=CW_CHECKPOINT_FILENAME))/1000)
            cw_tickets = CW.get_tickets(since_ts_epoch=CHECKPOINT_TS)
            l.info("Found CW [{}] tickets modified since: [{}]".format(len(cw_tickets), CHECKPOINT_TS))
            for cw_ticket in cw_tickets:
                cw_ticket_number = cw_ticket.get('id', '')
                cw_ticket_updated_str = cw_ticket.get('_info', {}).get('lastUpdated', '1970-01-01T00:00:00T')
                cw_ticket_updated_ts = CW.datestring_to_epoch(cw_ticket_updated_str)
                open_ticket = LDB.get_ticket_linkage(remote_ticket_id=cw_ticket_number)
                if open_ticket and not open_ticket.get('state', '') == 'closed':
                    rt_ticket_number = cw_ticket_number
                    rt_ticket_last_modified = open_ticket.get('remote_ticket_last_modified', '')
                    stellar_case_id = open_ticket.get('stellar_case_id', '')
                    if cw_ticket_updated_ts > rt_ticket_last_modified:
                        l.info("CW ticket has been modified since last sync: [{}] [ticket updated: {}] [last sync: {}]".format(rt_ticket_number, cw_ticket_updated_str, rt_ticket_last_modified))

                        ''' check on ticket resolution '''
                        if CW_SYNC_STATUS:
                            cw_status = cw_ticket.get('status', {}).get('name', '')
                            stellar_status = ''
                            if cw_status in CW_SYNC_STATUS_MAP:
                                stellar_status = CW_SYNC_STATUS_MAP.get(cw_status, '')
                            else:
                                stellar_status = CW_SYNC_STATUS_MAP.get('default', '')
                            if stellar_status.lower() in ["resolved", "cancelled"]:
                                l.info("CW ticket in state [{} {}] | closing related stellar case: [{}]".format(rt_ticket_number, cw_status, stellar_case_id))
                                SU.resolve_stellar_case(case_id=stellar_case_id, update_alerts=True)
                                LDB.close_ticket_linkage(stellar_case_id=stellar_case_id)
                            else:
                                l.info("CW ticket in state [{} {}] | updating related stellar case: [{}]".format(rt_ticket_number, cw_status, stellar_case_id))
                                SU.update_stellar_case (case_id=stellar_case_id, case_status=stellar_status, update_tag=False)
                                LDB.update_remote_ticket_timestamp(stellar_case_id=stellar_case_id, rt_ticket_ts=cw_ticket_updated_ts)

                        ''' check on ticket ownership '''
                        if CW_SYNC_OWNER:
                            if CW_FORCE_OWNER_SYNC:
                                ''' force owner sync '''
                                owner_link = cw_ticket.get('owner', {}).get('_info', {}).get('member_href', '')
                                if owner_link:
                                    new_owner_email = CW.get_member_email_via_link(owner_link)
                                    SU.update_stellar_case_assignee(case_id=stellar_case_id, case_assignee=new_owner_email)
                                    LDB.update_remote_ticket_timestamp(stellar_case_id=stellar_case_id, rt_ticket_ts=cw_ticket_updated_ts)
                                    l.info("Updated stellar case with assignee: [{}] [{}]".format(stellar_case_id, new_owner_email))

                            else:
                                ''' get ownership changes from audit records '''
                                owner_record = CW.get_ticket_ownership_change(rt_ticket_number)
                                if owner_record:
                                    owner_record_ts_str = owner_record.get('enteredDate', "1970-01-01T00:00:00Z")
                                    owner_record_ts = CW.datestring_to_epoch(owner_record_ts_str)
                                    if owner_record_ts > rt_ticket_last_modified:
                                        owner_link = cw_ticket.get('owner', {}).get('_info', {}).get('member_href', '')
                                        if owner_link:
                                            new_owner_email = CW.get_member_email_via_link(owner_link)
                                            SU.update_stellar_case_assignee(case_id=stellar_case_id, case_assignee=new_owner_email)
                                            LDB.update_remote_ticket_timestamp(stellar_case_id=stellar_case_id, rt_ticket_ts=cw_ticket_updated_ts)
                                            l.info("Updated stellar case with assignee: [{}] [{}]".format(stellar_case_id,
                                                                                                       new_owner_email))

                        ''' check on new notes '''
                        if CW_SYNC_NOTES:
                            ''' pull notes '''
                            cw_ticket_notes = CW.get_ticket_notes(ticket_id=rt_ticket_number)
                            for cw_ticket_note in cw_ticket_notes:
                                cw_note_id = cw_ticket_note.get('id', 0)
                                cw_note_text = cw_ticket_note.get('text')
                                cw_note_ts_str = cw_ticket_note.get('_info', {}).get('lastUpdated', "2025-01-01T00:00:00Z")
                                cw_note_ts = CW.datestring_to_epoch(cw_note_ts_str)
                                if cw_note_ts > rt_ticket_last_modified:
                                    l.info("Updating stellar case: [{}] with ticket note id: [{}]".format(stellar_case_id, cw_note_id))
                                    SU.add_case_comment(case_id=stellar_case_id, comment=cw_note_text)
                                    LDB.update_remote_ticket_timestamp(stellar_case_id=stellar_case_id, rt_ticket_ts=cw_ticket_updated_ts)

                        ''' check on audit items '''
                        if CW_SYNC_AUDIT_RECORDS:
                            ''' pull audit records  '''
                            cw_audit_records = CW.get_audit_records(ticket_id=rt_ticket_number)
                            for cw_audit_record in cw_audit_records:
                                cw_ar_text = cw_audit_record.get('text', '')
                                cw_ar_entered_by = cw_audit_record.get('enteredBy', '')
                                cw_ar_audit_type = cw_audit_record.get('auditType', '')
                                cw_ar_audit_subtype = cw_audit_record.get('auditSubType', '')
                                cw_ar_audit_source = cw_audit_record.get('auditSource', '')
                                # cw_note_ts_str = cw_audit_record.get('enteredDate')
                                cw_note_ts_str = cw_audit_record.get('enteredDate', "1970-01-01T00:00:00Z")
                                cw_note_ts = CW.datestring_to_epoch(cw_note_ts_str)
                                if cw_note_ts > rt_ticket_last_modified:
                                    stellar_comment_string = 'CW audit record\nType: {} Subtype: {} Time: {} By: {}\n[{}]'.format(
                                        cw_ar_audit_type, cw_ar_audit_subtype, cw_note_ts_str, cw_ar_entered_by, cw_ar_text)
                                    l.info("Updating stellar case: [{}] with ticket audit record: [{} / {}]".format(stellar_case_id, cw_note_ts_str, cw_ar_entered_by))
                                    SU.add_case_comment(case_id=stellar_case_id, comment=stellar_comment_string)
                                    LDB.update_remote_ticket_timestamp(stellar_case_id=stellar_case_id, rt_ticket_ts=cw_ticket_updated_ts)

            ''''''
            ''' Complete CW loop                            '''
            ''''''
            SU.checkpoint_write(filepath=CW_CHECKPOINT_FILENAME, val=NEW_CHECKPOINT_TS)


            ''''''
            ''' get all STELLAR cases since last checkpoint '''
            ''''''
            NEW_CHECKPOINT_TS = int(time() * 1000)
            CHECKPOINT_TS = int(SU.checkpoint_read(filepath=STELLAR_CHECKPOINT_FILENAME))

            # cases = SU.get_stellar_cases(from_ts=1707541200000)
            cases = SU.get_stellar_cases(from_ts=CHECKPOINT_TS, use_modified_at=True)
            for case in cases.get('cases', {}):
                stellar_case_id = case.get("_id")

                ''' if the case is already sync'd - skip over '''
                syncd_case = LDB.get_ticket_linkage(stellar_case_id=stellar_case_id)
                if syncd_case:
                    continue

                stellar_case_number = case.get('ticket_id')
                case_name = case.get('name', '')
                case_score = case.get('score', 0)
                case_tenant_name = case.get('tenant_name')
                case_summary = SU.get_case_summary(case_id=stellar_case_id)
                event_names = SU.get_case_alerts(stellar_case_id, return_only_alert_names=True)
                stellar_url = SU.make_stellar_case_url(stellar_case_id)
                l.info(
                    "Stellar Case ID: [{}] | Ticket Number: [{}] | URL: [{}]".format(stellar_case_id, stellar_case_number,
                                                                                     stellar_url))
                new_ticket_id = CW.create_ticket(ticket_summary=case_name, company_name=case_tenant_name, event_score=case_score, stellar_case_number=stellar_case_number)
                if new_ticket_id:
                    ticket_note_text = CW.create_ticket_note_text(case_summary=case_summary,
                                                                  case_tenant_name=case_tenant_name, case_url=stellar_url,
                                                                  alerts=event_names)
                    CW.create_ticket_note(ticket_id=new_ticket_id, ticket_note_text=ticket_note_text)
                    stellar_comment = "Connectwise ticket created: [{}]".format(new_ticket_id)
                    SU.update_stellar_case(case_id=stellar_case_id, case_comment=stellar_comment)
                    LDB.put_ticket_linkage(stellar_case_id=stellar_case_id, stellar_case_number=stellar_case_number, remote_ticket_id=new_ticket_id)
                else:
                    l.error("Failed to create Connectwise ticket - see log messages for more information")

            SU.checkpoint_write(filepath=STELLAR_CHECKPOINT_FILENAME, val=NEW_CHECKPOINT_TS)
            ts_loop_duration = time() - ts_start_of_loop
            if POLL_INTERVAL > ts_loop_duration:
                ts_sleep_time = POLL_INTERVAL - ts_loop_duration
                l.info("Process loop duration took {}s - sleeping: {}s".format(ts_loop_duration, ts_sleep_time))
                sleep(ts_sleep_time)
            else:
                l.warning("Process loop duration took longer than sleep time - staying awake to catch up")

    except Exception as e:
        l.error(traceback.format_exc())
        exit(1)

