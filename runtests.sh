#!/usr/bin/env bash

nosetests drow --with-coverage --cover-inclusive --cover-branches --cover-erase --cover-package drow $@ || { exit $?; }
