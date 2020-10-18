from draftfast import rules
from draftfast.rules import RuleSet
from draftfast.optimize import run, run_multi
from draftfast.orm import Player, Roster
from draftfast.csv_parse import salary_download, uploaders
from draftfast.dke_exceptions import *
from draftfast.settings import OptimizerSettings, Stack
from draftfast.lineup_constraints import LineupConstraints

import os
import pandas as pd
from datetime import datetime


class MME(object):
    SITE_MAP = {
        'dk': {
            'header': "DK",
            'rule': "DRAFT_KINGS",
            'uploader_name': "DraftKings",
        },
        'fd': {
            'header': 'FD',
            'rule': 'FAN_DUEL',
            'uploader_name': "FanDuel",
        }
    }

    def __init__(
            self,
            site: str = "",
            league: str = "",
            targets: dict = {},
            banned: list = [],
            stacks: list = [],
            exposures: list = [],
            project: str = "",
            additional_rules: dict = {},
            ownership_weight: float = 0.7,
            value_weight: float = 0.3,
            entries: int = 20,
            clean_projections: bool = False,
    ):
        self.site = site
        self.league = league
        self.targets = targets
        self.banned = banned
        self.stacks = stacks
        self.exposures = exposures
        self.entries = entries
        self.additional_rules = additional_rules
        if project:
            self.project = project
        else:
            self.project = os.getcwd()

        if ownership_weight + value_weight > 1:
            raise InvalidBoundsException("Sum of owernship and value weights must equal 1.")

        self.ownership_weight = ownership_weight
        self.value_weight = value_weight
        self.file_locations = self._get_file_locations()
        self.clean_projections = clean_projections

    def __repr__(self):

        return "MME project for {league} on {site} on {d}".format(
            league=self.league,
            site=self.site,
            d=(datetime.today()).strftime("%Y%m%d")
        )

    def _get_default_rules(self) -> RuleSet:
        return getattr(
            rules,
            "{site}_{league}_RULE_SET".format(
                site=self.SITE_MAP[self.site]['header'],
                league=self.league
            )
        )

    def _get_file_locations(self) -> dict:

        return {
            "salaries": os.path.join(self.project, "data", self.site, self.league, "{}_salaries.csv".format(self.site)),
            "pid": os.path.join(self.project, "data", self.site, self.league, "{}_pid.csv".format(self.site)),
            "upload": os.path.join(self.project, "data", self.site, self.league, "{}_upload.csv".format(self.site)),
            "projections": os.path.join(self.project, "data", self.site, self.league,
                                        "{}_projections.csv".format(self.site))
        }

    def _adjust_projections(self) -> str:

        projection_file = self.file_locations['projections']

        if self.clean_projections:
            return projection_file

        site_header_value = self.SITE_MAP[self.site]['header']

        try:
            projections = pd.read_csv(projection_file)
        except FileNotFoundError:
            raise ("Projection file not found at {}".format(projection_file))

        try:
            projections['ownership'] = projections['{} Ownership'.format(site_header_value)].map(
                lambda x: x.replace('%', '')
            ).astype(int).map(lambda x: x + 1)

            projections['ownership_rank'] = 2.01 - projections.groupby('{} Position'.format(site_header_value))[
                'ownership'].apply(
                lambda x: (x - min(x)) / (max(x) - min(x))
            )

            projections['value_rank'] = projections.groupby('{} Position'.format(site_header_value))[
                '{} Value'.format(site_header_value)].apply(
                lambda x: (x - min(x)) / (max(x) - min(x))
            )

            projections['o_v'] = self.ownership_weight * projections['ownership_rank'] + self.value_weight * \
                                 projections['value_rank']

            projections['weight'] = projections.groupby("{} Position".format(site_header_value))['o_v'].apply(
                lambda x: 1 + (x - min(x)) / (max(x) - min(x))
            )
            projections['adjusted_points'] = projections['weight'] * projections[
                '{} Projection'.format(site_header_value)]
        except ValueError as e:
            raise ("Projection file parsing error".format(e))

        projection_output = projections[
            [
                'Player', '{} Position'.format(site_header_value),
                '{} Ownership'.format(site_header_value),
                '{} Value'.format(site_header_value),
                '{} Projection'.format(site_header_value),
                'adjusted_points'
            ]
        ]
        projection_output = projection_output.rename(
            columns={
                'Player': 'playername',
                'adjusted_points': 'points'
            }
        )

        projection_output = projection_output[projection_output['playername'].notna()]

        projection_output.to_csv(projection_file, index=False)

        self.clean_projections = True

        return projection_file
        # return "Projeciton files have been adjusted for {} players.".format(len(projection_output['playername'].unique()))

    def _compile_params(self):

        if self.stacks:
            optimizers = OptimizerSettings(
                stacks=[
                    Stack(
                        team=good['team'],
                        count=good['count'],
                        stack_lock_pos=good['lock'],
                        stack_eligible_pos=good['eligible']
                    )
                    for good in self.stacks
                ],
            )
        else:
            optimizers = OptimizerSettings()

        exposures = self.exposures

        groups = []

        if self.targets:
            for k in self.targets.keys():
                groups.append(
                    [
                        self.targets[k]['players'],
                        (
                            self.targets[k]['lower'],
                            self.targets[k]['upper']
                        )
                    ]
                )

        if self.banned or groups:
            constraints = LineupConstraints(
                banned=self.banned,
                groups=groups
            )
        else:
            constraints = LineupConstraints()

        return constraints, exposures, optimizers

    def _set_additional_rules(self) -> RuleSet:
        default_ruleset = self._get_default_rules()
        if 'salary_max' in self.additional_rules:
            default_ruleset.salary_max = self.additional_rules['salary_max']
        if 'salary_min' in self.additional_rules:
            default_ruleset.salary_max = self.additional_rules['salary_min']
        if 'position_limit' in self.additional_rules:
            new_position_limit = []
            for position_1 in default_ruleset.position_limits:
                for position_2 in self.additional_rules['position_limit'].keys():
                    if position_1[0] == position_2:
                        position_1[1] = self.additional_rules['position_limit'][position_2][0]
                        position_1[2] = self.additional_rules['position_limit'][position_2][1]
                    else:
                        new_position_limit.append(position_1)
                    new_position_limit.append(position_1)
            default_ruleset.position_limits = new_position_limit

        if not default_ruleset:
            raise ValueError("Errors in customizing rules")

        return default_ruleset

    def _generate_players(self) -> list:

        salary_file = self.file_locations['salaries']
        projection_file = self._adjust_projections()
        rule = getattr(rules, self.SITE_MAP[self.site]['rule'])
        try:
            return salary_download.generate_players_from_csvs(
                salary_file_location=salary_file,
                projection_file_location=projection_file,
                game=rule,
            )
        except MissingPlayersException as e:
            raise ("Error: {}".format(e))

    def _generate_rosters(self) -> Roster:

        players = self._generate_players()
        custom_rules = self._set_additional_rules()

        try:
            constraints, exposures, optimizers = self._compile_params()
        except NameError as e:
            raise ("Constrains error {}".format(e))
        try:
            return run_multi(
                iterations=self.entries,
                exposure_bounds=exposures,
                rule_set=custom_rules,
                player_pool=players,
                optimizer_settings=optimizers,
                constraints=constraints,
                verbose=True,
            )
        except InvalidBoundsException as e:
            raise ("Error: {}".format(e))

    def generate_entries(self) -> str:

        rosters = self._generate_rosters()
        print(rosters)
        pid_file = self.file_locations['pid']
        upload_file = self.file_locations['upload']
        new_uploader = getattr(
            uploaders,
            "{name}{league}Uploader".format(
                name=self.SITE_MAP[self.site]['uploader_name'],
                league=self.league
            )
        )
        try:
            uploader = new_uploader(
                pid_file=pid_file,
                upload_file=upload_file
            )
            uploader.write_rosters(rosters[0])
        except InvalidBoundsException as e:
            raise ('Error: {}'.format(e))

        return "{} rosters were generated".format(len(rosters[0]))