#!/usr/bin/env bash


if [[ $TRAVIS_OS_NAME != 'osx' ]]; then
    export PYENV_ROOT="$HOME/.pyenv"
    export PATH="$PYENV_ROOT/bin:$PATH"
fi

eval "$(pyenv init -)"
eval "$(pyenv virtualenv-init -)"
pyenv activate jpy-venv
python --version

# oracle-java8-set-default seems to modify PATH but not JAVA_HOME :(
java -version
if [[ "$TRAVIS_OS_NAME" == "osx" ]]; then 
    export JAVA_HOME=$(/usr/libexec/java_home -v 1.8);
else
    export JAVA_HOME=/usr/lib/jvm/java-8-oracle     
fi
echo $JAVA_HOME

python setup.py --maven bdist_wheel
