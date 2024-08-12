import requests
from datetime import datetime
from prometheus_client import start_http_server, Gauge
import time
import logging
from dotenv import load_dotenv
from statistics import mean
import os

# Load environment variables from .env file
load_dotenv()

# Configuration for logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# GitHub repository details
OWNER = os.getenv('GITHUB_OWNER')
REPO = os.getenv('GITHUB_REPO')
TOKEN = os.getenv('GITHUB_TOKEN')

# Headers for GitHub API authentication
HEADERS = {'Authorization': f'token {TOKEN}'}

# Prometheus metrics
DEPLOYMENT_GAUGE = Gauge(
    'github_deployments_total',
    'Total number of GitHub deployments',
    ['branch', 'repo', 'status']
)

CHANGE_FAILURE_RATE = Gauge(
    'github_change_failure_rate',
    'Percentage of failed GitHub deployments relative to the total number of deployments',
    ['branch', 'repo']
)

LEAD_TIME_GAUGE = Gauge(
    'github_deployments_lead_time',
    'Lead time for changes in seconds',
    ['branch', 'repo']
)

MTTR_GAUGE = Gauge(
    'github_deployments_mttr',
    'Mean Time to Recovery (MTTR) in seconds',
    ['branch', 'repo']
)

DEPLOYMENT_DURATION_SUM = Gauge(
    'github_deployments_duration_sum',
    'Total deployment duration sum in seconds',
    ['branch', 'repo', 'status']
)

DEPLOYMENT_DURATION = Gauge(
    'github_deployments_duration',
    'Deployment duration in seconds',
    ['branch', 'repo', 'status']
)

def fetch_data_in_chunks(url, params=None):
    """
    Fetch data from GitHub API in chunks to handle large datasets.
    """
    while url:
        try:
            response = requests.get(url, headers=HEADERS, params=params)
            response.raise_for_status()
            yield response.json()
            url = response.links.get('next', {}).get('url')
        except requests.RequestException as e:
            logger.error(f'Error fetching data: {e}')
            break

def fetch_workflow_runs():
    """
    Fetch workflow runs in chunks.
    """
    logger.info('Fetching workflow runs from GitHub repository')
    url = f'https://api.github.com/repos/{OWNER}/{REPO}/actions/runs'
    params = {'status': 'completed', 'per_page': 100, 'event': 'push'}

    for data in fetch_data_in_chunks(url, params):
        yield from data['workflow_runs']

def calculate_deployment_counter(deployments):
    """
    Increment the deployment counter based on the deployment status.
    """
    logger.info('Calculating deployment counters')

    deployment_stats = {
        'dev': {'success': 0, 'failure': 0, 'in_progress': 0, 'queued': 0, 'waiting': 0},
        'staging': {'success': 0, 'failure': 0, 'in_progress': 0, 'queued': 0, 'waiting': 0},
        'main': {'success': 0, 'failure': 0, 'in_progress': 0, 'queued': 0, 'waiting': 0}
    }

    for deployment in deployments:
        if deployment['path'] in ['.github/workflows/dev-deployment.yml', '.github/workflows/staging-deployment.yml', '.github/workflows/production-deployment.yml']:
            branch = deployment['head_branch']
            status = deployment['conclusion']

            if branch in deployment_stats and status in deployment_stats[branch]:
                deployment_stats[branch][status] += 1
                logger.debug(f'Incremented deployment counter for {branch}, status: {status}')

    for branch, statuses in deployment_stats.items():
        for status, count in statuses.items():
            DEPLOYMENT_GAUGE.labels(branch=branch, repo=REPO, status=status).set(count)

        success_count = statuses['success']
        failure_count = statuses['failure']
        total_count = success_count + failure_count
        
        # Calculate failure percentage
        if total_count > 0:
            failure_percentage = (failure_count / total_count) * 100
        else:
            failure_percentage = 0
        
        # Update Change Failure Rate metric
        CHANGE_FAILURE_RATE.labels(branch=branch, repo=REPO).set(failure_percentage)

    logger.info('Deployment counters updated successfully')

def calculate_lead_time_for_changes(deployments):
    """
    Calculate lead time for changes for different branches and update the gauge.
    """
    logger.info('Calculating lead time for changes')
    branch_lead_times = {'dev': [], 'staging': [], 'main': []}

    branch_paths = {
        '.github/workflows/dev-deployment.yml': 'dev',
        '.github/workflows/staging-deployment.yml': 'staging',
        '.github/workflows/production-deployment.yml': 'main'
    }

    for deployment in deployments:
        deployment_branch = deployment['head_branch']
        deployment_conclusion = deployment['conclusion']
        deployment_path = deployment['path']
        
        if deployment_conclusion == 'success' and deployment_path in branch_paths:
            deployment_started_at = datetime.strptime(deployment['run_started_at'], '%Y-%m-%dT%H:%M:%SZ')        
            deployment_time = datetime.strptime(deployment['updated_at'], '%Y-%m-%dT%H:%M:%SZ')
            lead_time = (deployment_time - deployment_started_at).total_seconds()

            branch = branch_paths[deployment_path]
            branch_lead_times[branch].append(lead_time)
            logger.debug(f'Calculated lead time for workflows on branch {branch}: {lead_time} seconds')

    for branch, times in branch_lead_times.items():
        if times:
            average_lead_time = mean(times)
            LEAD_TIME_GAUGE.labels(branch=branch, repo=REPO).set(average_lead_time)
            logger.info(f'Updated lead time gauge for branch {branch}, average lead time: {average_lead_time} seconds')
        else:
            LEAD_TIME_GAUGE.labels(branch=branch, repo=REPO).set(0)
            logger.info(f'No lead times recorded for branch {branch}, set to 0 seconds')

def calculate_mttr(runs):
    """
    Calculate Mean Time to Recovery (MTTR) for different branches and update the gauge.
    """
    logger.info('Calculating MTTR')
    branch_recovery_times = {'dev': [], 'staging': [], 'main': []}

    for run in runs:
        if run['conclusion'] == 'success':
            branch = run['head_branch']
            valid_branches = {'dev', 'staging', 'main'}
            if branch not in valid_branches:
                continue

            previous_failures = [r for r in runs if r['updated_at'] < run['updated_at'] and r['conclusion'] == 'failure' and r['head_branch'] == branch]

            if previous_failures:
                last_failure = max(previous_failures, key=lambda r: r['updated_at'])
                recovery_time = (datetime.strptime(run['updated_at'], '%Y-%m-%dT%H:%M:%SZ') - datetime.strptime(last_failure['updated_at'], '%Y-%m-%dT%H:%M:%SZ')).total_seconds()
                branch_recovery_times[branch].append(recovery_time)
                logger.debug(f'Calculated MTTR for branch {branch}: {recovery_time} seconds')

    for branch, times in branch_recovery_times.items():
        if times:
            average_mttr = sum(times) / len(times)
            MTTR_GAUGE.labels(branch=branch, repo=REPO).set(average_mttr)
            logger.info(f'Updated MTTR gauge for branch {branch}, average MTTR: {average_mttr} seconds')
        else:
            MTTR_GAUGE.labels(branch=branch, repo=REPO).set(0)
            logger.info(f'No recovery times recorded for branch {branch}, set to 0 seconds')

def calculate_deployment_duration(deployments):
    """
    Calculate deployment duration and update the gauges.
    """
    logger.info('Calculating deployment durations')
    branch_durations = {
        'dev': {'success': 0, 'failure': 0, 'in_progress': 0, 'queued': 0, 'waiting': 0},
        'staging': {'success': 0, 'failure': 0, 'in_progress': 0, 'queued': 0, 'waiting': 0},
        'main': {'success': 0, 'failure': 0, 'in_progress': 0, 'queued': 0, 'waiting': 0}
    }
    branch_durations_count = {
        'dev': {'success': 0, 'failure': 0, 'in_progress': 0, 'queued': 0, 'waiting': 0},
        'staging': {'success': 0, 'failure': 0, 'in_progress': 0, 'queued': 0, 'waiting': 0},
        'main': {'success': 0, 'failure': 0, 'in_progress': 0, 'queued': 0, 'waiting': 0}
    }

    for deployment in deployments:
        branch = deployment['head_branch']
        status = deployment['conclusion']
        path = deployment['path']

        if branch in branch_durations and status in branch_durations[branch]:
            deployment_started_at = datetime.strptime(deployment['run_started_at'], '%Y-%m-%dT%H:%M:%SZ')
            deployment_ended_at = datetime.strptime(deployment['updated_at'], '%Y-%m-%dT%H:%M:%SZ')
            duration = (deployment_ended_at - deployment_started_at).total_seconds()

            if path in ['.github/workflows/dev-deployment.yml', '.github/workflows/staging-deployment.yml', '.github/workflows/production-deployment.yml']:
                branch_durations[branch][status] += duration
                branch_durations_count[branch][status] += 1
                logger.debug(f'Updated duration for branch {branch}, status {status}: {duration} seconds')

    for branch, durations in branch_durations.items():
        for status, total_duration in durations.items():
            count = branch_durations_count[branch][status]
            if count > 0:
                average_duration = total_duration / count
                DEPLOYMENT_DURATION_SUM.labels(branch=branch, repo=REPO, status=status).set(total_duration)
                DEPLOYMENT_DURATION.labels(branch=branch, repo=REPO, status=status).set(average_duration)
                logger.info(f'Updated deployment duration gauges for branch {branch}, status {status}: total {total_duration} seconds, average {average_duration} seconds')
            else:
                DEPLOYMENT_DURATION_SUM.labels(branch=branch, repo=REPO, status=status).set(0)
                DEPLOYMENT_DURATION.labels(branch=branch, repo=REPO, status=status).set(0)
                logger.info(f'No deployments for branch {branch}, status {status}, set durations to 0')

def main():
    # Start Prometheus metrics server
    port = int(os.getenv('PORT', 8000))
    start_http_server(port)
    logger.info(f'Started Prometheus metrics server on port {port}')

    while True:
        try:
            deployments = list(fetch_workflow_runs())
            calculate_deployment_counter(deployments)
            calculate_lead_time_for_changes(deployments)
            calculate_mttr(deployments)
            calculate_deployment_duration(deployments)
        except Exception as e:
            logger.error(f'Error occurred: {e}')

        # Sleep for a while before checking again
        time.sleep(300)

if __name__ == '__main__':
    main()
