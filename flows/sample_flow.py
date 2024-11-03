from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner


@task
def job1_flow():
    print('do flow1-task1')
    print('do flow1-task2')
    return 'return job1'


@task
def job2_flow():
    print('do job2-task')


@task
def job3_flow():
    print('do job3-task')


@task
def job4_flow():
    print('do job4-task1')
    print('do job4-task2')


@flow
def overall_flow():
    job1_future = job1_flow.submit()
    print('job1 submitted')
    job2_future = job2_flow.submit()
    print('job2 submitted')
    result = job1_future.result()
    print(f'job1 finished. job1 result={result}')
    job2_future.wait()
    print(f'job2 finished.')

    job3_flow()
    job4_flow()


if __name__ == '__main__':
    overall_flow()
