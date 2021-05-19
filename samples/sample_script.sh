#!/usr/bin/env sh

# set -xv

# show usage and quit
function usage()
{
  printf "usage: %s <command>\n" $(basename $0) >&2
  printf "\n" >&2
  printf "    commands are:\n"
  printf "        download [<version>] - downloads and mounts all example data\n" >&2
  printf "                               gets latest version unless <version> supplied\n" >&2
  printf "        remove - removes all example data\n" >&2
  printf "        version - show local version\n" >&2
  printf "        versions - list available versions\n" >&2
  exit 2
}


# complain and quit
function fail_out()
{
  printf "Failed! %s\n" "$@" >&2
  exit 2
}


# check that we have the expected enlistment directory; download if not
function ensure_enlistment()
{
  if [ ! -d $target_path/.git ]; then
     printf "no examples collection at %s; dowloading ..." $target_path >&2
     do_download
  fi
}


# clone the git repo, don't report progress but DO report errors
function do_download()
{
  if [ -d $target_path/.git ]; then
    printf "examples collection already exists at %s\n" $target_path >&2
  else
    git clone --quiet $git_root_url $target_path || fail_out "Couldn't clone examples repository"
    printf "examples downloaded to $target_path\n"
  fi
  [ ! -z "$1" ] && do_checkout_version "$1"
}


# remove the enlistment directory
function do_remove()
{
  [ -d $target_path ] || fail_out "Couldn't find $target_path$root_path"
  rm -rf $target_path
  printf "$target_path removed\n"
}


# list all the tags we know
function do_list_versions()
{
  cd $target_path
  printf "local versions follow:\n"
  git tag -n
  printf "remote versions follow:\n"
  git ls-remote --tags $git_root_url | grep -v "{}" | awk '{print $2}' | sed 's/refs\/tags\///'
  printf "Version listed\n"
}


# switch version to something different
function do_checkout_version()
{
  cd $target_path
  git -c advice.detachedHead=false checkout "$1" || fail_out "Couldn't change versions"
  printf "set to version %s\n" "$1"
}


#####
# set up the source and target info
git_root_url="git://github.com/deephaven/examples.git"
target_path="/data/examples"

# figure out command and dispatch ...
case "$1" in
  download)
    do_download "$2"
    ;;
  version)
    [ -z "$2" ] && fail_out "need a version specification"
    ensure_enlistment
    do_checkout_version "$2"
    ;;
  versions)
    ensure_enlistment
    do_list_versions
    ;;
  remove)
    do_remove
    ;;
  *)
    printf "Unknown command '%s'\n" $1 >&2
    usage
    ;;
esac
