#!/usr/bin/env bash

if [[ $TRAVIS_OS_NAME == 'osx' ]]; then

    # Install some custom requirements on OS X
    # e.g. brew install pyenv-virtualenv
    # See https://gist.github.com/Bouke/11261620
    # and https://github.com/bincrafters/conan-bazel_installer

    brew update || brew update
    brew outdated pyenv || brew upgrade pyenv
    brew install pyenv-virtualenv
    eval "$(pyenv init -)"
    eval "$(pyenv virtualenv-init -)"

    pyenv install --list
    pyenv install --skip-existing $PYTHON_VERSION
    pyenv virtualenv $PYTHON_VERSION jpy-venv

else
    # Install pyenv
    # See https://github.com/pyenv/pyenv
    git clone https://github.com/pyenv/pyenv.git $HOME/.pyenv
    export PYENV_ROOT="$HOME/.pyenv"
    export PATH="$PYENV_ROOT/bin:$PATH"
    eval "$(pyenv init -)"

    # Install pyenv virtualenv plugin
    # See https://github.com/pyenv/pyenv-virtualenv
    git clone https://github.com/pyenv/pyenv-virtualenv.git $(pyenv root)/plugins/pyenv-virtualenv
    eval "$(pyenv virtualenv-init -)"

    # Create virtualenv from current Python
    pyenv virtualenv jpy-venv
fi

pyenv rehash
pyenv activate jpy-venv
pip install wheel



