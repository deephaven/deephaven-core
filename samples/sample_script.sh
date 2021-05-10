#!/usr/bin/env sh

# show usage and quit
function usage ()
{
  printf "usage: %s <command> [<sample_name>]\n" $(basename $0) >&2
  printf "\n" >&2
  printf "    <command> is one of: download, remove\n" >&2
  printf "    <sample_name> is one of: iris, gsod, metriccentury\n" >&2
  printf "\n" >&2
  printf "    commands are:\n"
  printf "        download - downloads and mounts the sample data\n" >&2
  printf "        remove - removes the sample data\n" >&2
  exit 2
}


# complain and quit
function fail_out ()
{
  printf "Failed! %s\n" $1 >&2
  exit 2
}


# figure out command
case $1 in
  download)    do_download=1
               ;;
  remove)      do_remove=1
               ;;
  *)  printf "Unknown command '%s'\n" $2 >&2
      usage
      ;;
esac


# set up the source and target info
git_root_url="https://github.com/deephaven/examples/"
target_path="/data/"
case $2 in
  metriccentury)   root_path="metriccentury"
                   source_file="metriccentury.tar.gz"
                   source_file_path="raw/main/$source_file"
                   ;;
  gsod)            root_path="gsod"
                   source_file="gsod.tar.gz"
                   source_file_path="raw/main/$source_file"
                   ;;
  iris)            root_path="iris"
                   source_file="iris.tar.gz"
                   source_file_path="raw/main/$source_file"
                   ;;
  *)   printf "Unknown sample name '%s'\n" $2 >&2
       usage
       ;;
esac


# get to work ...
if ! [ -z $do_download ]
then
  wget -q "$git_root_url$source_file_path" --output-document=$source_file || fail_out "Couldn't download $git_root_url$source_file_path"
  tar -zxf $source_file --directory $target_path || fail_out "Couldn't unpack $source_file"
  rm $source_file
  printf "$root_path downloaded\n"
fi

if ! [ -z $do_remove ]
then
  [ -d $target_path$root_path ] || fail_out "Couldn't find $target_path$root_path"
  rm -r $target_path$root_path
  printf "$root_path removed\n"
fi


