from fabric.api import lcd, local, settings, task, puts, hide
from fabric.colors import green
import os

ROOT_DIR = os.path.abspath(os.path.dirname(__file__))


def info(text):
    puts(green(text))


@task
def polish():
    with lcd(ROOT_DIR):
        with settings(hide('running'), warn_only=True):
            # Remove compiled python classes
            info('Removing compiled python classes...')
            local('pyclean ./r5d4 ./tests ./scripts fabfile.py run.py')
            local('find ./r5d4 ./tests ./scripts fabfile.py run.py '
                  '-name "*.py[co]" -print0 | xargs -0 rm -f')

            # Fix file permissions
            info('Fixing file permissions...')
            local('find . '
                  '-path ./.git -prune -o '
                  '-path ./venv -prune -o '
                  '\( -name "analytics.py" -o '
                  '-name "analytics_manager.py" -o '
                  '-name "analytics_worker.py" -o '
                  '-name "run.py" -o '
                  '-wholename "./scripts/add_keys.py" -o '
                  '-wholename "./tests/publish.py" -o '
                  '-wholename "./tests/run_tests.py" \) '
                  '-exec chmod 0755 {} \\; -o '
                  '-type d -exec chmod 0755 {} \\; -o '
                  '-type f -exec chmod 0644 {} \\;')

            # Run coding standards check
            info('Running coding standards check...')
            local('pep8 ./r5d4 ./tests ./scripts fabfile.py run.py')

            # Run static code analyzer
            info('Running static code analyzer...')
            local('pyflakes ./r5d4 ./tests ./scripts fabfile.py run.py')

            # Find merge conflict leftovers
            info('Finding merge conflict leftovers...')
            local('! find . '
                  '-path ./.git -prune -o '
                  '-path ./venv -prune -o '
                  '-wholename "./fabfile.py" -o '
                  '-type d -o '
                  '-print0 | '
                  'xargs -0 grep -Pn "<<<<|====|>>>>"')

            # Check for debug print statements
            info('Checking for debug print statements...')
            local('! find . -type f '
                  '-path ./.git -prune -o '
                  '-path ./venv -prune -o '
                  '-wholename "./fabfile.py" -o '
                  '-name "*.py" -print0 | '
                  'xargs -0 grep -Pn \'(?<![Bb]lue)print\'')

            # Run Tests
            info('Running tests...')
            local('PYTHONPATH="%s" ./tests/run_tests.py' % ROOT_DIR)

            # Remove compiled python classes
            info('Removing compiled python classes...')
            local('pyclean .')
            local('find . -name "*.py[co]" -print0 | xargs -0 rm -f')
