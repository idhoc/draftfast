from draftfast import rules
from draftfast.optimize import run, run_multi
from draftfast.orm import Player, Roster
from draftfast.csv_parse import salary_download, uploaders
from draftfast.dke_exceptions import *
from draftfast.settings import OptimizerSettings, Stack
from draftfast.lineup_constraints import LineupConstraints

import pandas as pd
import csv

# TODO: Convert into class and class methods

def adjust_projections(projection_file, site='DK', ownership_weight=0.3, value_weight=0.7):
    projections = pd.read_csv(projection_file)
    projections['ownership'] = projections['{} Ownership'.format(site)].map(
        lambda x: x.replace('%', '')
    ).astype(int).map(lambda x: x+1)
    projections['ownership_rank'] = 2.01-projections.groupby('{} Position'.format(site))['ownership'].apply(
        lambda x: (x-min(x))/(max(x)-min(x))
    )
    projections['value_rank'] = projections.groupby('{} Position'.format(site))['{} Value'.format(site)].apply(
        lambda x: (x-min(x))/(max(x)-min(x))
    )
    projections['o_v'] = ownership_weight*projections['ownership_rank'] + value_weight*projections['value_rank']
    projections['weight'] = projections.groupby("{} Position".format(site))['o_v'].apply(
        lambda x: 1+(x-min(x))/(max(x)-min(x))
    )
    projections['adjusted_points'] = projections['weight']*projections['{} Projection'.format(site)]
    
    projection_output = projections[
        [
            'Player', '{} Position'.format(site), '{} Ownership'.format(site), 
            '{} Value'.format(site), '{} Projection'.format(site), 'adjusted_points'
        ]
    ]
    projection_output = projection_output.rename(
        columns = {
            'Player': 'playername',
            'adjusted_points': 'points'
        }
    )
    
    projection_output.to_csv(projection_file)
    
    return True


def make_params(teams, ban_list, target_wrs, target_rbs, exposures):

    stacks=[Stack(team=good['team'], count=good['count'], stack_lock_pos=good['lock'], stack_eligible_pos=good['eligible']) for good in teams]

    optimizers = OptimizerSettings(
        stacks=stacks,
    )

    banned_list = ban_list

    target_wr = target_wrs
    target_rb = target_rbs

    exposures = exposures

    constraints = LineupConstraints(
        banned=banned_list,
        groups=[
            [target_wr['players'], (target_wr['lower'], target_wr['upper'])],
            [target_rb['players'], (target_rb['lower'], target_rb['upper'])],
        ]
    )
    return constraints, exposures, optimizers


# DK MME
def make_dk_nfl(
    entries=20, salary_file=None, pid_location=None, projection_file=None, 
    teams=[], ban_list=[], target_wrs=[], target_rbs=[], exposures=[]
):
    
    if not salary_file:
        salary_file = "./data/da/DKSalaries.csv"
        
    if not pid_location:
        pid_location = "./data/da/DKEntries.csv"
        
    if not projection_file:
        projection_file = "./data/da/da_projections.csv"

    # Get player list
    try:
        players = salary_download.generate_players_from_csvs(
            salary_file_location=salary_file,
            projection_file_location=projection_file,
            game=rules.DRAFT_KINGS,
        )
    except MissingPlayersException as e:
        raise("Error: {}".format(e))
    
    # Modify default DK rules
    custom_dk_nfl_rules = rules.DK_NFL_RULE_SET
#     custom_dk_nfl_rules.salary_min = 3000
#     custom_dk_nfl_rules.position_limits = [['QB', 1, 1], ['RB', 2, 3], ['WR', 3, 4], ['TE', 1, 1], ['DST', 1, 1]]
    
    try:
        constraints, exposures, optimizers = make_params(teams, ban_list, target_wrs, target_rbs, exposures)
        rosters = run_multi(
            iterations=entries,
            exposure_bounds=exposures,
            rule_set=custom_dk_nfl_rules,
            player_pool=players,
            optimizer_settings=optimizers,
            constraints=constraints,
            verbose=True,
        )
    except InvalidBoundsException as e:
        raise("Error: {}".format(e))
    
    try:
        uploader = uploaders.DraftKingsNFLUploader(
            pid_file=pid_location,
        )
        uploader.write_rosters(rosters[0])
    except InvalidBoundsException as e:
        raise('Error: {}'.format(e))
        
    return "{} rosters were generated".format(len(rosters[0]))

# FD MME
def make_fd_nfl(
    entries=20, salary_file=None, pid_location=None, projection_file=None, 
    teams=[], ban_list=[], target_wrs=[], target_rbs=[], exposures=[]
):
    
    if not salary_file:
        salary_file = "./data/affiliate/af_salaries.csv"
        
    if not pid_location:
        pid_location = "./data/affiliate/af_salaries.csv"
        
    if not projection_file:
        projection_file = "./data/affiliate/af_projections.csv"

    # Get player list
    try:
        players = salary_download.generate_players_from_csvs(
            salary_file_location=salary_file,
            projection_file_location=projection_file,
            game=rules.FAN_DUEL,
        )
    except MissingPlayersException as e:
        raise("Error: {}".format(e))
    
    # Modify default DK rules
    custom_fd_nfl_rules = rules.FD_NFL_RULE_SET
#     custom_dk_nfl_rules.position_limits = [['QB', 1, 1], ['RB', 2, 3], ['WR', 3, 4], ['TE', 1, 1], ['DST', 1, 1]]
    
    try:
        constraints, exposures, optimizers = make_params(teams, ban_list, target_wrs, target_rbs, exposures)
        rosters = run_multi(
            iterations=entries,
            exposure_bounds=exposures,
            rule_set=custom_fd_nfl_rules,
            player_pool=players,
            optimizer_settings=optimizers,
            constraints=constraints,
            verbose=True,
        )
    except InvalidBoundsException as e:
        raise("Error: {}".format(e))
    
    try:
        uploader = uploaders.FanDuelNFLUploader(
            pid_file=pid_location,
        )
        uploader.write_rosters(rosters[0])
    except InvalidBoundsException as e:
        raise('Error: {}'.format(e))
        
    return "{} rosters were generated".format(len(rosters[0]))
