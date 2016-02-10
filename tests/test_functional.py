import subprocess

from docopt import docopt
from datarobot_sdk.models.modeljob import wait_for_async_model_creation
from datarobot_batch_scoring import batch_scoring


def test_args_smoke(temp_project, user):
    # train one model in project
    trainable = temp_project.get_blueprints()[0]
    model = wait_for_async_model_creation(temp_project.id,
                                          temp_project.train(trainable,
                                                             sample_pct=64))
    arguments = ('--host http://127.0.0.1/api'
                 ' --user {username}'
                 ' --password {password}'
                 ' {project_id}'
                 ' {model_id}'
                 ' tests/fixtures/temperatura_predict.csv'
                 ' --n_samples 10'
                 ' --n_concurrent 1').format(username=user[0],
                                             password=user[1],
                                             project_id=temp_project.id,
                                             model_id=model.id)
    docopt(batch_scoring.__doc__, arguments)


def test_args_from_subprocess(temp_project, user):
    # train one model in project
    trainable = temp_project.get_blueprints()[0]
    model = wait_for_async_model_creation(temp_project.id,
                                          temp_project.train(trainable,
                                                             sample_pct=64))
    arguments = ('batch_scoring --host http://127.0.0.1/api'
                 ' --user {username}'
                 ' --password {password}'
                 ' {project_id}'
                 ' {model_id}'
                 ' tests/fixtures/temperatura_predict.csv'
                 ' --n_samples 10'
                 ' --n_concurrent 1').format(username=user[0],
                                             password=user[1],
                                             project_id=temp_project.id,
                                             model_id=model.id)

    assert 0 == subprocess.call(arguments.split(' '))
