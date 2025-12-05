__version__ = '20251205.000'

'''
    Provides methods to call ConnctWise API for incident creation and update

    20240205.000    initial
    20251201.000    several updates and enhancements to get_company
    20251202.000    added option to add tenant_name to summary line
    20251203.000    fixed small bug involving default_board
    20251203.001    added method for getting audit records
    20251203.002    changed the method for getting notes to allNotes (which works better for some reason)
    20251204.000    added methods to support ticket ownership change events
    20251204.001    added truncation for max ticket summary length of 100
    20251205.000    added method get_tickets which retrieves tickets modified since TS

'''

import requests
import json
from datetime import datetime

class ConnectWise:

    def __init__(self, logger, config={}, public_key=''):

        self.l = logger
        self.l.info('Stellar-ConnectWise version: [{}]'.format(__version__))

        self.cw_host = config.get('cw_host')
        self.cw_company_id = config.get('cw_company_id')
        self.cw_private_key = config.get('cw_private_key')
        self.cw_public_key = config.get('cw_public_key')
        self.cw_client_id = config.get('cw_client_id')

        ticket_config = config.get('ticket', {})
        self.cw_default_company = ticket_config.get('default_company', '')
        self.cw_avoid_company_lookup = ticket_config.get('avoid_company_lookup', False)
        self.cw_default_board = ticket_config.get('default_board', '')
        self.cw_use_default_board = ticket_config.get('avoid_board_lookup', False)
        self.ticket_prefix = ticket_config.get('summary_prefix', '')
        # added 20251202.000 to support tenant name in prefix
        self.ticket_prefix_includes_tenant_name = ticket_config.get('summary_prefix_includes_tenant_name', False)
        # added 20251203.001 to support case number in prefix
        self.ticket_prefix_includes_case_number = ticket_config.get('summary_prefix_includes_case_number', False)
        # added 20220721.000 to support ticket status
        self.cw_ticket_status = ticket_config.get('status', '')
        if self.cw_ticket_status == 'New':
            self.cw_ticket_status = ''

        self.tenant_map = config['tenant_map']

        # support for SLA and event_score added 20230301
        self.sla = config.get('SLA', {})
        self.headers = {'Accept': 'application/json', 'Content-type': 'application/json',
                     'clientId': '{}'.format(self.cw_client_id)}
        self.auth = ('{}+{}'.format(self.cw_company_id, self.cw_public_key), '{}'.format(self.cw_private_key))
        self.base_url = 'https://{}/{}/apis/3.0'.format(self.cw_host, self._get_company_info())

        # preload the default (fallback) company to avoid useless lookups
        self.cw_default_company_id = self.get_default_company_id()


    def get_version(self):
        return __version__

    def test_connection(self):
        ret = False
        url = "{}{}".format(self.base_url, '/system/info')
        self.l.info("Testing connection to: [{}]".format(url))
        r = requests.get(url=url, headers=self.headers, auth=self.auth)
        rr = json.loads(r.text)
        printable_r = json.dumps(rr, indent=4, sort_keys=True)
        # self.l.debug(printable_r)
        if 'version' in rr:
            self.l.info("Successful connectivity test. Connectwise version: [{}]".format(rr['version']))
            ret = True
        else:
            self.l.error("Connectivity test FAILED - cannot continue")
            self.l.error("{} {}".format(r.status_code, rr))
            raise Exception("Connectivity test FAILED - cannot continue")

        return ret

    def get_ticket(self, ticket_id):
        _URL_ = self.base_url
        _AUTH_ = self.auth
        _HEADERS_ = self.headers
        l = self.l
        l.info("Getting ticket: [{}]".format(ticket_id))
        rr = {}
        url = "{}/service/tickets/{}".format(_URL_, ticket_id)
        r = requests.get(url=url, headers=_HEADERS_, auth=_AUTH_)
        if 200 <= r.status_code <= 299:
            rr = json.loads(r.text)
            # printable_r = json.dumps(rr, indent=4, sort_keys=True)
            # l.debug(printable_r)
            # print(printable_r)
        else:
            l.error("Error retrieving CW ticket id: {} [{}: {}]".format(ticket_id, r.status_code, r.text))
        return rr

    def get_tickets(self, since_ts_epoch):
        _URL_ = self.base_url
        _AUTH_ = self.auth
        _HEADERS_ = self.headers
        since_ts_str = self._epoch_to_datestring(since_ts_epoch)
        rr = {}
        if since_ts_str:
            self.l.info("Getting tickets since: [{}] UTC".format(since_ts_str))
            url = '{}/service/tickets?conditions=lastUpdated > "{}"'.format(_URL_, since_ts_str)
            r = requests.get(url=url, headers=_HEADERS_, auth=_AUTH_)
            if 200 <= r.status_code <= 299:
                rr = json.loads(r.text)
            else:
                self.l.error("Error retrieving CW tickets: [{}: {}]".format(r.status_code, r.text))
        else:
            self.l.error("Cannot get ticket - echoch to string broken: [{}]".format(since_ts_epoch))
        return rr

    def create_ticket(self, ticket_summary, company_name, board_name='', event_score=0, stellar_case_number=None):
        new_ticket_id = 0
        tenant_name = company_name
        company_id = self.get_company(company_name)
        (priority_name, priority_id) = self.get_ticket_priority(event_score)
        summary_string = ticket_summary
        if self.ticket_prefix:
            summary_string = '{} {}'.format(self.ticket_prefix, summary_string)
        if self.ticket_prefix_includes_tenant_name and tenant_name:
            summary_string = '[{}] {}'.format(tenant_name, summary_string)
        if self.ticket_prefix_includes_case_number and stellar_case_number:
            summary_string = '[{}] {}'.format(stellar_case_number, summary_string)
        if priority_name:
            summary_string = '[{}] {}'.format(priority_name, summary_string)
        # truncate to CW max summary len of 100 chars
        summary_string = "{:.99}".format(summary_string)
        ticket_data = {
            'summary': '{}'.format(summary_string),
            'company': {
                'id': company_id
            },
            # added 20220721.000 to support ticket status
            'status': {
                'name': '{}'.format(self.cw_ticket_status)
            }
        }

        if self.cw_use_default_board:
            ticket_data['board'] = {"id": self.get_board(self.cw_default_board)}
            # ticket_data['board'] = {"name": '{}'.format(self.cw_default_board)}
        else:
            ticket_data['board'] = {"id": self.get_board(board_name)}

        # support for event_score and priority_id added 20230301
        if priority_id:
            ticket_data['priority'] = {'id': priority_id}
        ticket_data = json.dumps(ticket_data)

        url = '{}{}'.format(self.base_url, '/service/tickets')
        r = requests.post(url=url, headers=self.headers, auth=self.auth, data=ticket_data)
        if 200 <= r.status_code <= 299:
            rr = json.loads(r.text)
            new_ticket_id = int(rr['id'])
            self.l.info("New ticket created: [{}]".format(new_ticket_id))
        else:
            self.l.error("Error creating ticket: [{}: {}]".format(r.status_code, r.text))

        return new_ticket_id

    def get_companies(self):
        url = '{}/company/companies?fields=id,name,status&pageSize=1000'.format(self.base_url)
        # url = '{}/company/companies?name="Microsoft"'.format(_URL_)
        # url = '{}/company/companies?conditions=name="Microsoft"&fields=id,name,status'.format(_URL_)
        r = requests.get(url=url, headers=self.headers, auth=self.auth)
        if 200 <= r.status_code <= 299:
            rr = json.loads(r.text)
            printable_r = json.dumps(rr, indent=4, sort_keys=True)
            # l.debug(printable_r)
            # print(printable_r)
            item_cnt = 0
            for r_item in rr:
                c_id = r_item['id']
                c_name = r_item['name']
                print("company id: [{}] name: [{}]".format(c_id, c_name))
                item_cnt += 1
        else:
            self.l.error("Error querying companies: [{}: {}]".format(r.status_code, r.text))

        print("count: {}".format(item_cnt))
        return

    def get_company(self, company_name, last_try=False, all_fields=False):
        ret_id = 0
        if self.cw_avoid_company_lookup:
            self.l.info("Using default company for all tickets as optioned: [{}/{}]".format(self.cw_default_company, self.cw_default_company_id))
            ret_id = self.cw_default_company_id
        else:
            self.l.info("Finding company name: [{}]".format(company_name))
            if self.tenant_map and company_name in self.tenant_map:
                mapped_company_name = self.tenant_map[company_name]
                self.l.info("Tenant name: [{}] mapped to CW company: [{}]".format(company_name, mapped_company_name))
                company_name = mapped_company_name
            if all_fields:
                url = '{}/company/companies?conditions=name="{}"'.format(self.base_url, company_name)
            else:
                url = '{}/company/companies?conditions=name="{}"&fields=id,name,status,deletedFlag'.format(self.base_url,
                                                                                                           company_name)
            r = requests.get(url=url, headers=self.headers, auth=self.auth)
            if 200 <= r.status_code <= 299:
                rr = json.loads(r.text)
                for c in rr:
                    if 'id' in c:
                        df = c['deletedFlag']
                        if df:
                            self.l.warning("Company: [{} / {}] is flagged as deleted - skipping".format(c['id'], company_name))
                            continue
                        ret_id = c['id']
                        self.l.info("Found company id: [{}]".format(ret_id))
                        break
            else:
                self.l.error("Error querying companies: [{}: {}]".format(r.status_code, r.text))

            if not ret_id:
                self.l.warning("Company name [{}] could not be found. Using default company: [{}/{}]".format(
                        company_name, self.cw_default_company, self.cw_default_company_id))
                ret_id = self.cw_default_company_id

        return ret_id

    def get_default_company_id(self):
        ret_id = 0
        company_name = self.cw_default_company
        if company_name:
            self.l.info("Finding default company name: [{}]".format(company_name))
            url = '{}/company/companies?conditions=name="{}"&fields=id,name,status,deletedFlag'.format(self.base_url, company_name)
            r = requests.get(url=url, headers=self.headers, auth=self.auth)
            if 200 <= r.status_code <= 299:
                rr = json.loads(r.text)
                for c in rr:
                    if 'id' in c:
                        df = c['deletedFlag']
                        if df:
                            self.l.warning("Company: [{} / {}] is flagged as deleted - skipping".format(c['id'], company_name))
                            continue
                        ret_id = c['id']
                        self.l.info("Found company id: [{}]".format(ret_id))
                        break
            else:
                self.l.error("Error querying companies: [{}: {}]".format(r.status_code, r.text))

            if not ret_id:
                raise Exception("Default company not identified: [{}] - cannot proceed".format(company_name))
        else:
            raise Exception("Default company name not specified; cannot proceed")
        return ret_id

    def get_boards(self):
        self.l.info("Getting all board names")
        url = '{}/service/boards?fields=id,name,status&pageSize=1000'.format(self.base_url)
        r = requests.get(url=url, headers=self.headers, auth=self.auth)
        if 200 <= r.status_code <= 299:
            rr = json.loads(r.text)
            item_cnt = 0
            for c in rr:
                if 'id' in c:
                    ret_id = c['id']
                    b_name = c['name']
                    # b_status = c['status']
                    print("Board id: [{}] name: [{}]".format(ret_id, b_name))
                    item_cnt += 1

        # printable_r = json.dumps(rr, indent=4, sort_keys=True)
        # l.debug(printable_r)
        # print(printable_r)
        else:
            self.l.error("Error querying boards: [{}: {}]".format(r.status_code, r.text))

        print("count: {}".format(item_cnt))
        return

    def get_board(self, board_name, last_try=False):
        self.l.info("Finding board name: [{}]".format(board_name))
        ret_id = 0
        url = '{}/service/boards?conditions=name="{}"&fields=id,name,status'.format(self.base_url, board_name)
        r = requests.get(url=url, headers=self.headers, auth=self.auth)
        if 200 <= r.status_code <= 299:
            rr = json.loads(r.text)
            for c in rr:
                if 'id' in c:
                    ret_id = c['id']
                    self.l.info("Found board id: [{}]".format(ret_id))
                    break
        # printable_r = json.dumps(rr, indent=4, sort_keys=True)
        # l.debug(printable_r)
        # print(printable_r)
        else:
            self.l.error("Error querying boards: [{}: {}]".format(r.status_code, r.text))

        if not ret_id:
            if last_try:
                raise Exception("Service Board Name lookup failure: [{}]. Cannot create ticket.".format(board_name))
            else:
                # lookup the default company
                self.l.warning("Board name [{}] could not be found. Looking up the default board: [{}]".format(board_name,
                                                                                                          self.cw_default_board))
                ret_id = self.get_board(self.cw_default_board, last_try=True)

        return ret_id

    def get_priorities(self):
        self.l.info("Getting all priorities")
        url = '{}/service/priorities?fields=id,name,status&pageSize=1000'.format(self.base_url)
        r = requests.get(url=url, headers=self.headers, auth=self.auth)
        if 200 <= r.status_code <= 299:
            rr = json.loads(r.text)
            item_cnt = 0
            for c in rr:
                if 'id' in c:
                    ret_id = c['id']
                    b_name = c['name']
                    # b_status = c['status']
                    print("Priority id: [{}] name: [{}]".format(ret_id, b_name))
                    item_cnt += 1

            printable_r = json.dumps(rr, indent=4, sort_keys=True)
            # l.debug(printable_r)
            print(printable_r)
        else:
            self.l.error("Error querying priorities: [{}: {}]".format(r.status_code, r.text))

        # print("count: {}".format(item_cnt))
        return

    def get_ticket_priority(self, event_score):
        # try:
        event_score = int(event_score)
        ticket_sev = ''
        ticket_priority_id = 0
        sla = self.sla
        if sla:
            if event_score >= sla['CRITICAL']['min']:
                ticket_sev = 'CRITICAL'
            elif event_score >= sla['HIGH']['min']:
                ticket_sev = 'HIGH'
            elif event_score >= sla['MED']['min']:
                ticket_sev = 'MED'
            else:
                ticket_sev = 'LOW'
            ticket_priority_id = sla[ticket_sev]['cw_priority_id']
        # except:
        # 	pass
        self.l.info("Setting ticket priority: [{}: {}]".format(ticket_priority_id, ticket_sev))
        return (ticket_sev, ticket_priority_id)

    def get_ticket_notes(self, ticket_id):
        _URL_ = self.base_url
        _AUTH_ = self.auth
        _HEADERS_ = self.headers
        l = self.l
        l.info("Getting ticket notes: [{}]".format(ticket_id))
        # url = "{}/service/tickets/{}/notes".format(_URL_, ticket_id)
        url = "{}/service/tickets/{}/allNotes".format(_URL_, ticket_id)
        r = requests.get(url=url, headers=_HEADERS_, auth=_AUTH_)
        if 200 <= r.status_code <= 299:
            rr = json.loads(r.text)
        else:
            self.l.error("Problem getting ticket notes: [{}]".format(ticket_id))
        return rr

    def create_ticket_note(self, ticket_id, ticket_note_text):
        _URL_ = self.base_url
        _AUTH_ = self.auth
        _HEADERS_ = self.headers
        l = self.l
        note_data = json.dumps(
            {
                'text': '{}'.format(ticket_note_text),
                'ticketId': ticket_id,
                'internalFlag': True,
                'externalFlag': False,
                "detailDescriptionFlag": True,
                "internalAnalysisFlag": False,
                "resolutionFlag": False
            }
        )
        url = '{}/service/tickets/{}/notes'.format(_URL_, ticket_id)
        r = requests.post(url=url, headers=_HEADERS_, auth=_AUTH_, data=note_data)
        if 200 <= r.status_code <= 299:
            rr = json.loads(r.text)
            ticket_note_id = int(rr['id'])
            l.info("New ticket note created: [ticket id: {} | note id: {}]".format(ticket_id, ticket_note_id))
        else:
            l.error("Error creating note for ticket: [{}: {}]".format(r.status_code, r.text))

        return ticket_note_id

    def get_audit_records(self, ticket_id):
        _URL_ = self.base_url
        _AUTH_ = self.auth
        _HEADERS_ = self.headers
        rr = {}
        self.l.info("Getting audit records: [{}]".format(ticket_id))
        # url = "{}/service/tickets/{}/notes".format(_URL_, ticket_id)
        url = "{}/system/audittrail?type=Ticket&id={}".format(_URL_, ticket_id)
        r = requests.get(url=url, headers=_HEADERS_, auth=_AUTH_)
        if 200 <= r.status_code <= 299:
            rr = json.loads(r.text)
        else:
            self.l.error("Problem getting audit records: [{}]".format(ticket_id))
        return rr

    def get_ticket_ownership_change(self, ticket_id):
        ret = {}
        audit_records = self.get_audit_records(ticket_id)
        # pick out only ownership record changes
        for ar in audit_records:
            if ar.get('auditType', '') == "Resource" and ar.get('auditSubType', '') == "Owner":
                ret = ar
                break
        return ret

    def get_member_email_via_link(self, member_link):
        ''' the direct member link is obtained from the ticket response json - owner '''
        _AUTH_ = self.auth
        _HEADERS_ = self.headers
        email = ''
        l = self.l
        l.info("Getting email for member via direct link: [{}]".format(member_link))
        rr = {}
        url = member_link
        r = requests.get(url=url, headers=_HEADERS_, auth=_AUTH_)
        if 200 <= r.status_code <= 299:
            rr = json.loads(r.text)
            email = rr.get('primaryEmail', '')
        else:
            l.error("Error retrieving direct member link: [{}]".format(member_link))
        return email

    def create_ticket_note_text(self, case_summary :str, case_tenant_name :str, case_url :str, alerts=[]):
        ticket_note_text = "{}\n\n{}\n\n{}\n\n".format(case_summary, case_tenant_name, case_url)
        for alert in alerts:
            ticket_note_text += "- {}\n".format(alert)
        return ticket_note_text

    def datestring_to_epoch(self, datestring):
        epoch_time = 0
        dformat = "%Y-%m-%dT%H:%M:%S%z"
        try:
            epoch_time = int(datetime.strptime(datestring, dformat).timestamp()) * 1000
        except(ValueError) as e:
            self.l.error("Time conversion error: [{}]".format(e))
        return epoch_time

    def _epoch_to_datestring(self, ts_epoch):
        ts_string = ''
        dformat = "%Y-%m-%dT%H:%M:%S%z"
        try:
            ts_dt = datetime.utcfromtimestamp(ts_epoch)
            ts_string = datetime.strftime(ts_dt, dformat)

        # (datetime.strptime(datestring, dformat).timestamp()) * 1000)
        except(ValueError) as e:
            self.l.error("Time conversion error: [{}]".format(e))

        return ts_string

    def _get_company_info(self):
        ret = ''
        url = 'https://{}/login/companyinfo/{}'.format(self.cw_host, self.cw_company_id)
        self.l.info("Obtaining codebase from: [{}]".format(url))
        r = requests.get(url=url, headers=self.headers)
        rr = json.loads(r.text)
        printable_r = json.dumps(rr, indent=4, sort_keys=True)
        self.l.debug(printable_r)
        if 'Codebase' in rr:
            ret = str(rr['Codebase']).strip("/")
            self.l.info("Codebase returned: [{}]".format(ret))
        else:
            self.l.error("No codebase returned - cannot continue")
            raise Exception("No codebase returned - cannot continue")
        return ret
