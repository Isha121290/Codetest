from jira.resources import Issue
from jira.client import JIRA
import configparser
import teradata
import warnings

warnings.filterwarnings("ignore")
    
config = configparser.ConfigParser()
config.read('Config.ini')

#Make a Teradata connection
host, username, password = config['Teradata']['host'], config['Teradata']['user'], config['Teradata']['passwd']
udaExec = teradata.UdaExec (appName="MyApp", version="1.0", logConsole=False)
connect = udaExec.connect(method="odbc", system=host, username=username, password=password, driver="Teradata Database ODBC Driver 16.00", authentication="LDAP")


#Make a Jira Connection
jira = JIRA(basic_auth=(config['Jira']['user'], config['Jira']['passwd']), options={'server': 'https://jira.kpn.org','verify':False})

issues_str = ''
type_of_issues_to_pull = [
                  'completedIssues', 
                  'incompletedIssues',
                  'issuesNotCompletedInCurrentSprint',
                  'issuesCompletedInAnotherSprint']


def sprint_issues(cls, board_id, sprint_id):
    r_json = cls._get_json('rapid/charts/sprintreport?rapidViewId=%s&sprintId=%s' % (board_id, sprint_id), base = cls.AGILE_BASE_URL)
    issue_list = []
    for t in type_of_issues_to_pull:
        if t in r_json['contents']:
            issue_list += [Issue(cls._options, cls._session, raw_issues_json) for raw_issues_json in r_json['contents'][t]]
    return {x.key: x for x in issue_list}.values()


boards = jira.boards()
board = [b for b in boards if b.name == 'Junglebook'][0]
sprints = jira.sprints(board.id)
connect.execute("DELETE FROM " + config['Teradata']['Env'] + "_TEC_EDW.CURRENT_SPRINT;")
for sprint in sprints:
    if str(sprint.state) == 'ACTIVE':
        issues = sprint_issues(jira, board.id, sprint.id)
        for issue in issues:
            print issue, issue.statusName, issue.summary
            connect.execute("INSERT INTO " + config['Teradata']['Env'] + "_TEC_EDW.CURRENT_SPRINT VALUES('" + str(issue) + "','" + str(issue.statusName) + "');")
