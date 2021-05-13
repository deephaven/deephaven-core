#!/usr/bin/env sh

# set -xv

# show usage and quit
function usage()
{
  printf "usage: %s <command>\n" $(basename $0) >&2
  printf "\n" >&2
  printf "    <command> is one of: download, remove\n" >&2
  printf "    <sample_name> is one of: iris, gsod, metriccentury\n" >&2
  printf "\n" >&2
  printf "    commands are:\n"
  printf "        download [<version>] - downloads and mounts all sample data\n" >&2
  printf "                               gets latest version unless <version> supplied\n" >&2
  printf "        remove - removes all sample data\n" >&2
  printf "        version - show local version\n" >&2
  printf "        versions - list available version\n" >&2
  exit 2
}


# complain and quit
function fail_out()
{
  printf "Failed! %s\n" "$@" >&2
  exit 2
}


# check that we have the expected enlistment directory; complain and fail if not
function require_enlistment()
{
  if [ ! -d $target_path/.git ]; then
     printf "no samples collection at %s; use the download command first\n" $target_path >&2
     fail_out
  fi
}


# clone the git repo, don't report progress but DO report errors
function do_download()
{
  git clone --quiet $git_root_url $target_path || fail_out "Couldn't clone samples repository"
  printf "samples downloaded to $target_path\n"
  [ ! -z "$1" ] && do_version "$1"
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
  git tag -n
  printf "Version listed\n"
}


# switch version to something different
function do_version()
{
  cd $target_path
  git -c advice.detachedHead=false checkout "$1" || fail_out "Couldn't change versions"
  printf "set to version %s\n" "$1"
}


#####
# set up the source and target info
git_root_url="git://github.com/mikeblas/samples-junk.git"
target_path="/data/samples"

# figure out command and dispatch ...
case "$1" in
  download)
    do_download "$2"
    ;;
  version)
    require_enlistment
    [ -z "$2" ] && fail_out "need a version specification"
    do_version "$2"
    ;;
  versions)
    require_enlistment
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

