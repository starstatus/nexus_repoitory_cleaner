#!/usr/bin/python
import operator
import requests
import datetime
import string
import re
import sys
import argparse
import json
import logging

LOGGER = logging.getLogger()
# arguments
arg_DRY_RUN = False
arg_USERNAME = "admin"
arg_PASSWORD = "admin123"


class ArtifactCleaner:

    def __init__(self):
        pass

    # Query Nexus Repository.
    def fetch_content(self, url):
        headers = {"accept": "application/json"}
        res = requests.get(url, auth=(arg_USERNAME, arg_PASSWORD), headers=headers)
        try:
            return res.json()
        except ValueError:
            # return an empty list
            return json.loads('{"data":[]}')

    # delete a version of an artifact
    def delete_content(self, version_url):
        LOGGER.info("deleting: " + version_url)
        if not arg_DRY_RUN:
            try:
                res = requests.delete(version_url, auth=(arg_USERNAME, arg_PASSWORD))
                LOGGER.info("deleting result: " + str(res.status_code))
            except:
                LOGGER.warn("Error deleting " + version_url)

    # rebuild metadata
    def rebuild_metadata(self, metadata_url):
        LOGGER.info("rebuilding metadata: " + metadata_url)
        if not arg_DRY_RUN:
            try:
                res = requests.delete(metadata_url, auth=(arg_USERNAME, arg_PASSWORD))
                LOGGER.info("rebuilding metadata result: " + str(res.status_code))
            except:
                LOGGER.info("Error rebuilding metadata " + metadata_url)

    # find artifact versions that are older than specific date.
    def find_artifacts_by_keep_date(self, artifact_url, date):
        content = self.fetch_content(artifact_url)

        version_urls = []
        for content_item in content["data"]:
            resource_uri = content_item["resourceURI"]
            text = content_item["text"]
            ext = text[string.rfind(text, ".") + 1:]

            is_leaf = content_item["leaf"]
            last_modified_date = datetime.datetime.strptime(content_item["lastModified"][:10], "%Y-%m-%d")
            if not is_leaf and date > last_modified_date:
                version_urls.append(resource_uri)

        return version_urls

    # find artifact versions that are NOT the latest N versions.
    def find_artifacts_by_keep_last(self, artifact_url, keep_last):
        content = self.fetch_content(artifact_url)

        # versions and old_versions are list of pairs (url, lastModifiedTime)
        versions = []
        old_versions = []
        for content_item in content["data"]:
            resource_uri = content_item["resourceURI"]
            # find all versions of the artifact url (added as a tuple: uri, last_modified)
            is_leaf = content_item["leaf"]
            if not is_leaf:
                last_modified_time = datetime.datetime.strptime(content_item["lastModified"][:19], "%Y-%m-%d %H:%M:%S")
                versions.append((resource_uri, last_modified_time))

        # sort and find versions that are NOT the latest N versions
        versions.sort(key=operator.itemgetter(1))
        size = len(versions)
        if keep_last < size:
            old_versions = versions[:size - keep_last]

        to_remove = []
        for ver in old_versions:
            to_remove.append(ver[0])
        return to_remove

    # find the metadata url based on the artifact url
    def find_metadata_url(self, artifact_url):
        temp = re.split("(?<=/service/local)/", artifact_url, maxsplit=1, flags=re.IGNORECASE)
        return temp[0] + "/metadata/" + temp[1]

    # remove all versions older than a specific date
    def clean_artifact_by_keep_date(self, artifact_url, standard_date):
        # find all repository items that's older than a certain date
        urls = self.find_artifacts_by_keep_date(artifact_url, standard_date)
        LOGGER.debug("Found artifacts by keep date: " + str(urls))

        # Delete each artifact via the Nexus Rest API.
        for url in urls:
            self.delete_content(url)

        if len(urls) > 0:
            self.rebuild_metadata(self.find_metadata_url(artifact_url))

    # remove artifacts; keep newest N versions
    def clean_artifact_by_keep_last(self, artifact_url, keep_number):
        # find all repository items which are NOT the latest N versions
        urls = self.find_artifacts_by_keep_last(artifact_url, keep_number)
        LOGGER.debug("Found artifacts by keep last: " + str(urls))

        # Delete each artifact via the Nexus Rest API.
        for url in urls:
            self.delete_content(url)

        if len(urls) > 0:
            self.rebuild_metadata(self.find_metadata_url(artifact_url))


def setup_args(argv):
    parser = argparse.ArgumentParser(description="parsing command line arguments...")
    parser.add_argument("-y", "--dryRun", action="store_true")
    parser.add_argument("--debug", action="store_true", help="show more debugging information.")
    parser.add_argument("-u", "--username", required=True)
    parser.add_argument("-p", "--password", required=True)
    parser.add_argument("-n", "--nexusUrl", required=True)
    parser.add_argument("-r", "--repositoryId", help="repository id. For example, public. ", required=True)
    parser.add_argument("-g", "--groupId", required=True)
    parser.add_argument("-a", "--artifactId", action="append", help="Can be used multiple times.", required=True)

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-l", "--keepLast", type=int, help="Keep latest N versions")
    group.add_argument("-d", "--keepDate", help="Delete all versions before this date. Format is YYYYMMDD.")

    return parser.parse_args(argv)


def setup_logging(debug_mode):
    handler = logging.StreamHandler(sys.stdout)
    LOGGER.addHandler(handler)
    if debug_mode:
        LOGGER.setLevel(logging.DEBUG)
    else:
        LOGGER.setLevel(logging.INFO)


if __name__ == "__main__":
    args = setup_args(sys.argv[1:])
    # args = setup_args("-u admin -p admin123 -n http://localhost:8080/nexus -r snapshots -g com.test -a test-api -d 20140501".split())

    arg_DRY_RUN = args.dryRun
    arg_USERNAME = args.username
    arg_PASSWORD = args.password

    setup_logging(args.debug)
    LOGGER.debug(sys.argv)

    if arg_DRY_RUN:
        LOGGER.info("Dry Run enabled.")

    arg_URLS = []
    # make the list of artifacts to be deleted
    for item in args.artifactId:
        repo = string.rstrip(args.nexusUrl, "/") + "/service/local/repositories/" \
             + args.repositoryId + "/content/" + string.replace(args.groupId, ".", "/") + "/" + item
        arg_URLS.append(repo)
    LOGGER.info("Will clean the following artifacts: " + str(arg_URLS))

    cleaner = ArtifactCleaner()
    if args.keepDate is not None:
        for url in arg_URLS:
            cleaner.clean_artifact_by_keep_date(url, datetime.datetime.strptime(args.keepDate, '%Y%m%d'))
    elif args.keepLast is not None:
        for url in arg_URLS:
            cleaner.clean_artifact_by_keep_last(url, args.keepLast)
    else:
        LOGGER.info("Please tell us how you want to clean the artifacts.")
