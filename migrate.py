"""
TAKP to PEQ Database Character Transfer Tool

This tool does not copy the following player tables

* account_flags
* account_rewards
* character_buffs
* character_consent
* character_corpse_items
* character_corpse_items_backup
* character_corpses
* character_corpses_backup
* character_inspect_messages
* character_lookup
* character_magelo_stats
* character_pet_buffs
* character_pet_info
* character_pet_inventory
* character_soulmarks
* character_timers
* character_zone_flags

* discovered_items
* friends
* guilds
* guild_ranks
* guild_members
* mail
* petitions
* player_titlesets
* quest_globals
* spell_globals
* client_version
* commands_log
* titles
* trader
"""
import argparse
import os
from enum import Enum
from os.path import join, dirname
from sqlalchemy import text, create_engine
from dotenv import load_dotenv

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path=dotenv_path)
# pylint: disable=no-member

DEBUG = True
HOST = os.environ.get("HOST")
USERNAME = os.environ.get("USERNAME")
PASSWD = os.environ.get("PASSWD")
EQEMU_DATABASE = os.environ.get("EQEMU_DATABASE")
EQMACEMU_DATABASE = os.environ.get("EQMACEMU_DATABASE")


class CharacterDoesNotExist(Exception):
    """Custom Exception for when a character can't be found in the target EQEMU_DATABASE"""


class TAKPInventorySlot(Enum):
    """Enum for Inventory Slots in the TAKP database"""
    # Equipment slots
    CURSOR = 0  # NOT CHARM - TAKP uses the cursor at slot 0
    LEFT_EAR = 1
    HEAD = 2
    FACE = 3
    RIGHT_EAR = 4
    NECK = 5
    SHOULDERS = 6
    ARMS = 7
    BACK = 8
    LEFT_WRIST = 9
    RIGHT_WRIST = 10
    RANGED = 11
    HANDS = 12
    PRIMARY = 13
    SECONDARY = 14
    LEFT_FINGER = 15
    RIGHT_FINGER = 16
    CHEST = 17
    LEGS = 18
    FEET = 19
    WAIST = 20
    AMMO = 21

    # General inventory slots (8 slots)
    GENERAL_1 = 22
    GENERAL_2 = 23
    GENERAL_3 = 24
    GENERAL_4 = 25
    GENERAL_5 = 26
    GENERAL_6 = 27
    GENERAL_7 = 28
    GENERAL_8 = 29

    # General bag slots (250-329: 8 bags × 10 slots)
    GENERAL_BAGS_BEGIN = 250
    GENERAL_BAGS_END = 329

    # Cursor bag slots (330-339: 1 bag × 10 slots)
    CURSOR_BAG_BEGIN = 330
    CURSOR_BAG_END = 339

    # Bank slots (8 slots)
    BANK_BEGIN = 2000
    BANK_END = 2007

    # Bank bag slots (2030-2109: 8 bags × 10 slots)
    BANK_BAGS_BEGIN = 2030
    BANK_BAGS_END = 2109

    # Trade slots
    TRADE_BEGIN = 3000
    TRADE_END = 3007

    # Trade bag slots
    TRADE_BAGS_BEGIN = 3030
    TRADE_BAGS_END = 3109


class PEQInventorySlot(Enum):
    """Enum for Inventory Slots in the PEQ database"""
    # Equipment slots
    CHARM = 0
    LEFT_EAR = 1
    HEAD = 2
    FACE = 3
    RIGHT_EAR = 4
    NECK = 5
    SHOULDERS = 6
    ARMS = 7
    BACK = 8
    LEFT_WRIST = 9
    RIGHT_WRIST = 10
    RANGED = 11
    HANDS = 12
    PRIMARY = 13
    SECONDARY = 14
    LEFT_FINGER = 15
    RIGHT_FINGER = 16
    CHEST = 17
    LEGS = 18
    FEET = 19
    WAIST = 20
    POWER_SOURCE = 21
    AMMO = 22

    # General inventory slots (10 slots)
    GENERAL_1 = 23
    GENERAL_2 = 24
    GENERAL_3 = 25
    GENERAL_4 = 26
    GENERAL_5 = 27
    GENERAL_6 = 28
    GENERAL_7 = 29
    GENERAL_8 = 30
    GENERAL_9 = 31
    GENERAL_10 = 32

    # Cursor
    CURSOR = 33

    # General bag slots (4010 - 6009: 10 bags × 200 slots, but only the first 8 used from TAKP)
    GENERAL_BAGS_BEGIN = 4010
    GENERAL_BAGS_COUNT = 10 * 200 # 2000
    GENERAL_BAGS_END = GENERAL_BAGS_BEGIN + GENERAL_BAGS_COUNT - 1 # 6009

    # Cursor bag slots (6010-6209): 200 slots
    CURSOR_BAG_BEGIN = 6010
    CURSOR_BAG_END = 6209

    # Tribute slots
    TRIBUTE_BEGIN = 400
    TRIBUTE_END = 404

    # Bank slots (24 slots, but only the first 8 used from TAKP)
    BANK_BEGIN = 2000
    BANK_END = 2023

    # Shared bank slots
    SHARED_BANK_BEGIN = 2500
    SHARED_BANK_END = 2501

    # Bank bag slots (6210-11009: 24 bags × 200 slots each)
    # Bank slot 2000 bag: 6210-6409
    # Bank slot 2001 bag: 6410-6609
    # etc.
    BANK_BAGS_BEGIN = 6210
    BANK_BAGS_END = 11009

    # Shared bank bag slots
    # Shared bank slot 2500 bag: 11010-11209
    # Shared bank slot 2501 bag: 11210-11409
    SHARED_BANK_BAGS_BEGIN = 11010
    SHARED_BANK_BAGS_END = 11409


def translate_slot_id_takp_to_peq(takp_slot: int) -> int:
    """
    Translate TAKP slot IDs to PEQ slot IDs.

    Args:
        takp_slot: TAKP inventory slot ID

    Returns:
        PEQ inventory slot ID
    """

    # Direct equipment slot mappings
    equipment_map = {
        TAKPInventorySlot.CURSOR.value: PEQInventorySlot.CURSOR.value,
        TAKPInventorySlot.LEFT_EAR.value: PEQInventorySlot.LEFT_EAR.value,
        TAKPInventorySlot.HEAD.value: PEQInventorySlot.HEAD.value,
        TAKPInventorySlot.FACE.value: PEQInventorySlot.FACE.value,
        TAKPInventorySlot.RIGHT_EAR.value: PEQInventorySlot.RIGHT_EAR.value,
        TAKPInventorySlot.NECK.value: PEQInventorySlot.NECK.value,
        TAKPInventorySlot.SHOULDERS.value: PEQInventorySlot.SHOULDERS.value,
        TAKPInventorySlot.ARMS.value: PEQInventorySlot.ARMS.value,
        TAKPInventorySlot.BACK.value: PEQInventorySlot.BACK.value,
        TAKPInventorySlot.LEFT_WRIST.value: PEQInventorySlot.LEFT_WRIST.value,
        TAKPInventorySlot.RIGHT_WRIST.value: PEQInventorySlot.RIGHT_WRIST.value,
        TAKPInventorySlot.RANGED.value: PEQInventorySlot.RANGED.value,
        TAKPInventorySlot.HANDS.value: PEQInventorySlot.HANDS.value,
        TAKPInventorySlot.PRIMARY.value: PEQInventorySlot.PRIMARY.value,
        TAKPInventorySlot.SECONDARY.value: PEQInventorySlot.SECONDARY.value,
        TAKPInventorySlot.LEFT_FINGER.value: PEQInventorySlot.LEFT_FINGER.value,
        TAKPInventorySlot.RIGHT_FINGER.value: PEQInventorySlot.RIGHT_FINGER.value,
        TAKPInventorySlot.CHEST.value: PEQInventorySlot.CHEST.value,
        TAKPInventorySlot.LEGS.value: PEQInventorySlot.LEGS.value,
        TAKPInventorySlot.FEET.value: PEQInventorySlot.FEET.value,
        TAKPInventorySlot.WAIST.value: PEQInventorySlot.WAIST.value,
        TAKPInventorySlot.AMMO.value: PEQInventorySlot.AMMO.value,
    }

    # General inventory slot mappings
    general_map = {
        TAKPInventorySlot.GENERAL_1.value: PEQInventorySlot.GENERAL_1.value,
        TAKPInventorySlot.GENERAL_2.value: PEQInventorySlot.GENERAL_2.value,
        TAKPInventorySlot.GENERAL_3.value: PEQInventorySlot.GENERAL_3.value,
        TAKPInventorySlot.GENERAL_4.value: PEQInventorySlot.GENERAL_4.value,
        TAKPInventorySlot.GENERAL_5.value: PEQInventorySlot.GENERAL_5.value,
        TAKPInventorySlot.GENERAL_6.value: PEQInventorySlot.GENERAL_6.value,
        TAKPInventorySlot.GENERAL_7.value: PEQInventorySlot.GENERAL_7.value,
        TAKPInventorySlot.GENERAL_8.value: PEQInventorySlot.GENERAL_8.value,
    }

    # Check equipment slots
    if takp_slot in equipment_map:
        return equipment_map[takp_slot]

    # Check general inventory slots
    if takp_slot in general_map:
        return general_map[takp_slot]

    # General bag slots: 250-329 → 4010 - 6009
    if TAKPInventorySlot.GENERAL_BAGS_BEGIN.value <= takp_slot <= TAKPInventorySlot.GENERAL_BAGS_END.value:
        offset = takp_slot - TAKPInventorySlot.GENERAL_BAGS_BEGIN.value # 250
        bag_num = offset // 10  # 0..7
        slot_in_bag = offset % 10  # 0..9

        # peq_slot = 4010 + bag_num * 200 + slot_in_bag
        return PEQInventorySlot.GENERAL_BAGS_BEGIN.value + bag_num * 200 + slot_in_bag

    # Cursor bag: 330-339 → 342-351
    if TAKPInventorySlot.CURSOR_BAG_BEGIN.value <= takp_slot <= TAKPInventorySlot.CURSOR_BAG_END.value:
        offset = takp_slot - TAKPInventorySlot.CURSOR_BAG_BEGIN.value
        return PEQInventorySlot.CURSOR_BAG_BEGIN.value + offset

    # Bank base slots: 2000-2007 (same in both systems)
    if TAKPInventorySlot.BANK_BEGIN.value <= takp_slot <= TAKPInventorySlot.BANK_END.value:
        return takp_slot

    # Bank bag slots: 2030-2109 → 6210-7809
    if TAKPInventorySlot.BANK_BAGS_BEGIN.value <= takp_slot <= TAKPInventorySlot.BANK_BAGS_END.value:
        bag_num = (takp_slot - TAKPInventorySlot.BANK_BAGS_BEGIN.value) // 10
        slot_in_bag = (takp_slot - TAKPInventorySlot.BANK_BAGS_BEGIN.value) % 10
        return PEQInventorySlot.BANK_BAGS_BEGIN.value + (bag_num * 200) + slot_in_bag

    # Unknown slot - log warning
    print(f"WARNING: Unknown TAKP slot {takp_slot}, returning unchanged")
    return takp_slot


class CharacterTransferTool:
    """Top level class that contains all the copy functions

    Example use:
        ctt = CharacterTransferTool('Soandso')
        ctt.copy_account()
        ...
    """
    ACCOUNT_TABLES = {
        'account': {'id_column': 'id', 'id_type': 'account'},
        'account_ip': {'id_column': 'accid', 'id_type': 'account'}
    }

    CHARACTER_TABLES = {
        'character_alternate_abilities': {'id_column': 'id', 'id_type': 'character'},
        'character_bind': {'id_column': 'id', 'id_type': 'character'},
        'character_currency': {'id_column': 'id', 'id_type': 'character'},
        'character_data': {'id_column': 'id', 'id_type': 'character'},
        'faction_values': {'id_column': 'char_id', 'id_type': 'character'},
        'inventory': {'id_column': 'character_id', 'id_type': 'character'},
        'character_languages': {'id_column': 'id', 'id_type': 'character'},
        'character_spells': {'id_column': 'id', 'id_type': 'character'},
        'character_memmed_spells': {'id_column': 'id', 'id_type': 'character'},
        'character_skills': {'id_column': 'id', 'id_type': 'character'}
    }

    def __init__(self, character_name: str):
        self.character_name = character_name

        self.eqemu_engine = create_engine(
            f"mysql+pymysql://{USERNAME}:{PASSWD}@{HOST}:3306/{EQEMU_DATABASE}"
        )

        self.eqmacemu_engine = create_engine(
            f"mysql+pymysql://{USERNAME}:{PASSWD}@{HOST}:3306/{EQMACEMU_DATABASE}"
        )

        self.new_peq_char_id = None  # Will be set after inserting character_data
        self.existing_peq_char_id = None  # Store existing ID for reuse
        self._load_character_info()

    def _load_character_info(self):
        """Load character and account info from the EQEMU database"""

        with self.eqmacemu_engine.connect() as conn:
            # Get character id, account id, and login server account id from character name
            sql = text("""
                       SELECT c.id, c.account_id, a.lsaccount_id
                       FROM character_data AS c
                                INNER JOIN account AS a
                                           ON a.id = c.account_id
                       WHERE c.name = :character_name
                       """)
            result = conn.execute(sql, {"character_name": self.character_name})
            row = result.fetchone()

            if not row:
                raise CharacterDoesNotExist(
                    f"Character '{self.character_name}' does not exist in the "
                    f"target EQEMU database. Check your EQEMU_DATABASE configuration."
                )
            self.old_char_id, self.new_account_id, self.ls_account_id = row
            print(f"Found character: old_char_id={self.old_char_id}, "
                  f"account_id={self.new_account_id}, "
                  f"ls_account_id={self.ls_account_id}")

    def __enter__(self):
        """Context manager support"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Clean up connections"""
        self.eqemu_engine.dispose()
        self.eqmacemu_engine.dispose()

    def clear_character_from_peqdb(self):
        """
        Clears character records from PEQ database by character name.

        This allows the script to be run idempotently - you can re-run it
        to update characters with fresh data from TAKP.

        Note: This only clears CHARACTER tables, not account tables.
        """
        with self.eqemu_engine.connect() as conn:
            # Find existing character in PEQ database by name
            sql = text("SELECT id FROM character_data WHERE name = :char_name")
            result = conn.execute(sql, {"char_name": self.character_name})
            row = result.fetchone()

            if row:
                existing_peq_char_id = row[0]
                self.existing_peq_char_id = existing_peq_char_id  # NEW: Save for reuse
                print(
                    f"Found existing character '{self.character_name}' in PEQ with ID {existing_peq_char_id}, deleting...")

                # Clear ONLY character tables using the existing PEQ character ID
                for table, config in self.CHARACTER_TABLES.items():
                    sql = text(f"DELETE FROM {table} WHERE {config['id_column']} = :char_id")
                    conn.execute(sql, {"char_id": existing_peq_char_id})

                conn.commit()
                print(f"Deleted existing character data for '{self.character_name}'")
            else:
                print(f"No existing character named '{self.character_name}' found in PEQ (first time import)")

    def copy_account(self):
        """
        Copies the compatible account table columns between a PEQ database and a TAKP database.
        Skips if account already exists in PEQ (idempotent).
        """
        # Check if account already exists in PEQ
        with self.eqemu_engine.connect() as conn:
            check_sql = text("SELECT id FROM account WHERE id = :account_id")
            result = conn.execute(check_sql, {"account_id": self.new_account_id})
            if result.fetchone():
                print(f"Account {self.new_account_id} already exists in PEQ, skipping account copy")
                return

        sql = text("SELECT * FROM account WHERE id = :new_account_id")
        sql = sql.bindparams(new_account_id=self.new_account_id)
        with self.eqmacemu_engine.connect() as conn:
            results = conn.execute(sql)

        insert_sql = text("INSERT INTO account (`id`, `name`, `charname`, `auto_login_charname`, "
                          "`sharedplat`, `password`,`status`, `ls_id`, `lsaccount_id`, `gmspeed`, "
                          "`invulnerable`, `flymode`, `ignore_tells`, `revoked`, `karma`, `minilogin_ip`, "
                          "`hideme`, `rulesflag`, `suspendeduntil`, `time_creation`, `ban_reason`, "
                          "`suspend_reason`) "
                          "VALUES (:id, :name, :charname, :auto_login_charname, :sharedplat, :password, "
                          ":status, :ls_id, :lsaccount_id, :gmspeed, :invulnerable, :flymode, "
                          ":ignore_tells, :revoked, :karma, :minilogin_ip, :hideme, :rulesflag, "
                          ":suspendeduntil, :time_creation, :ban_reason, :suspend_reason)")

        with self.eqemu_engine.connect() as conn:
            try:
                for record in results:
                    # noinspection PyProtectedMember
                    row = record._mapping
                    insert_sql = insert_sql.bindparams(id=row['id'],
                                                       name=row['name'],
                                                       charname=row['charname'],
                                                       auto_login_charname='',
                                                       sharedplat=row['sharedplat'],
                                                       password=row['password'],
                                                       status=row['status'],
                                                       ls_id='local',
                                                       lsaccount_id=row['lsaccount_id'],
                                                       gmspeed=row['gmspeed'],
                                                       invulnerable=row['gminvul'],
                                                       flymode=row['flymode'],
                                                       ignore_tells=row['ignore_tells'],
                                                       revoked=row['revoked'],
                                                       karma=row['karma'],
                                                       minilogin_ip=row['minilogin_ip'],
                                                       hideme=row['hideme'],
                                                       rulesflag=row['rulesflag'],
                                                       suspendeduntil=row['suspendeduntil'],
                                                       time_creation=row['time_creation'],
                                                       ban_reason=row['ban_reason'],
                                                       suspend_reason=row['suspend_reason'])
                    conn.execute(insert_sql)

                conn.commit()
                print(f"Copied account {self.new_account_id} to PEQ")
            except Exception as e:
                # If account already exists (duplicate name+ls_id), that's okay
                if "Duplicate entry" in str(e) and "name_ls_id" in str(e):
                    conn.rollback()
                    print(f"Account '{row['name']}' already exists in PEQ (duplicate name+ls_id), skipping")
                else:
                    raise

    def copy_account_ip(self):
        """
        Copies the compatible account_ip table columns from a TAKP database and a PEQ database.
        Skips if account_ip already exists in PEQ (idempotent).
        """
        # Check if account_ip already exists in PEQ
        with self.eqemu_engine.connect() as conn:
            check_sql = text("SELECT accid FROM account_ip WHERE accid = :account_id LIMIT 1")
            result = conn.execute(check_sql, {"account_id": self.new_account_id})
            if result.fetchone():
                print(f"Account IP records for account {self.new_account_id} already exist in PEQ, skipping")
                return

        sql = text("SELECT * FROM account_ip WHERE accid = :new_account_id")
        sql = sql.bindparams(new_account_id=self.new_account_id)
        with self.eqmacemu_engine.connect() as eqemu_conn:
            results = eqemu_conn.execute(sql)

        insert_sql = text("INSERT INTO account_ip (accid, ip, count, lastused) "
                          "VALUES (:accid, :ip, :count, :lastused)")

        with self.eqemu_engine.connect() as eqemu_conn:
            for record in results:
                # noinspection PyProtectedMember
                row = record._mapping
                insert_sql = insert_sql.bindparams(accid=row['accid'],
                                                   ip=row['ip'],
                                                   count=row['count'],
                                                   lastused=row['lastused'])
                eqemu_conn.execute(insert_sql)

            eqemu_conn.commit()
            print(f"Copied account_ip records for account {self.new_account_id} to PEQ")

    def copy_character_alternate_abilities(self):
        """
        Copies the compatible character_alternate_abilities table columns between a TAKP and PEQ database
        """
        sql = text("SELECT * from character_alternate_abilities WHERE id = :old_char_id")
        sql = sql.bindparams(old_char_id=self.old_char_id)
        with self.eqmacemu_engine.connect() as conn:
            results = conn.execute(sql)

        insert_sql = text("INSERT INTO character_alternate_abilities (id, aa_id, aa_value, charges) "
                          "VALUES (:id, :aa_id, :aa_value, :charges)")

        with self.eqemu_engine.connect() as conn:
            for record in results:
                # noinspection PyProtectedMember
                row = record._mapping
                insert_sql = insert_sql.bindparams(id=self.new_peq_char_id,
                                                   aa_id=row['aa_id'],
                                                   aa_value=row['aa_value'],
                                                   charges=0)
                conn.execute(insert_sql)

            conn.commit()

    def copy_character_bind(self):
        """
        Copies the compatible character_bind table columns between a PEQ
        database and a TAKP database
        """
        sql = text("SELECT * from character_bind WHERE id = :old_char_id")
        sql = sql.bindparams(old_char_id=self.old_char_id)
        with self.eqmacemu_engine.connect() as conn:
            results = conn.execute(sql)

        insert_sql = text("INSERT INTO character_bind (id, slot, zone_id, instance_id, x, y, z, heading) "
                          "VALUES (:id, :slot, :zone_id, :instance_id, :x, :y, :z, :heading)")

        with self.eqemu_engine.connect() as conn:
            for record in results:
                # noinspection PyProtectedMember
                row = record._mapping
                if row['is_home'] in (0, 1):
                    # TAKP is_home maps directly to PEQ slot:
                    # 0 = Bind Affinity location (death bind)
                    # 1 = Home/starting city bind
                    insert_sql = insert_sql.bindparams(id=self.new_peq_char_id,
                                                       slot=row['is_home'],
                                                       zone_id=row['zone_id'],
                                                       instance_id=0,
                                                       x=row['x'],
                                                       y=row['y'],
                                                       z=row['z'],
                                                       heading=row['heading'])
                    conn.execute(insert_sql)

            conn.commit()

    def copy_character_currency(self):
        """
        Copies TAKP character_currency table columns to the PEQ character_currency table
        """
        sql = text("SELECT * FROM character_currency WHERE id = :old_char_id")
        sql = sql.bindparams(old_char_id=self.old_char_id)
        with self.eqmacemu_engine.connect() as eqemu_conn:
            results = eqemu_conn.execute(sql)

        insert_sql = text("INSERT INTO character_currency (`id`, `platinum`, `gold`, `silver`, `copper`, "
                          "`platinum_bank`, `gold_bank`, `silver_bank`, `copper_bank`, `platinum_cursor`, "
                          "`gold_cursor`, `silver_cursor`, `copper_cursor`, `radiant_crystals`, `career_radiant_crystals`, "
                          "`ebon_crystals`, `career_ebon_crystals`) "
                          "VALUES (:id, :platinum, :gold, :silver, :copper, :platinum_bank, "
                          " :gold_bank, :silver_bank, :copper_bank, :platinum_cursor, :gold_cursor,"
                          " :silver_cursor, :copper_cursor, :radiant_crystals, :career_radiant_crystals, "
                          ":ebon_crystals, :career_ebon_crystals)")
        with self.eqemu_engine.connect() as eqmac_conn:
            for record in results:
                # noinspection PyProtectedMember
                row = record._mapping
                insert_sql = insert_sql.bindparams(id=self.new_peq_char_id,
                                                   platinum=row['platinum'],
                                                   gold=row['gold'],
                                                   silver=row['silver'],
                                                   copper=row['copper'],
                                                   platinum_bank=row['platinum_bank'],
                                                   gold_bank=row['gold_bank'],
                                                   silver_bank=row['silver_bank'],
                                                   copper_bank=row['copper_bank'],
                                                   platinum_cursor=row['platinum_cursor'],
                                                   gold_cursor=row['gold_cursor'],
                                                   silver_cursor=row['silver_cursor'],
                                                   copper_cursor=row['copper_cursor'],
                                                   radiant_crystals=0,
                                                   career_radiant_crystals=0,
                                                   ebon_crystals=0,
                                                   career_ebon_crystals=0
                                                   )
                eqmac_conn.execute(insert_sql)

            eqmac_conn.commit()

    def copy_character_data(self):
        """
        Copies TAKP character_data table columns to the PEQ character_data table

        TAKP → PEQ field mappings:
        - firstlogon → first_login (type differs)
        - showhelm → show_helm (default differs)

        PEQ expansion fields set to defaults:
        - zone_instance: 0
        - drakkin fields: 0
        - ability/discipline fields: 0
        - exp_enabled: 1
        - leadership fields: 0
        - ldon_points fields: 0
        - tribute fields: 0
        - extended PvP fields: 0
        - UI/consent fields: 0
        - AA legacy fields: 0
        - misc modern fields: 0

        TAKP fields lost: forum_id, boatid, boatname, famished, is_deleted, fatigue
        """
        sql = text("SELECT * FROM character_data WHERE id = :old_char_id")
        sql = sql.bindparams(old_char_id=self.old_char_id)
        with self.eqmacemu_engine.connect() as takp_conn:
            results = takp_conn.execute(sql)

            # Look up what the account ID is in PEQ for this account name
            # The account should exist from copy_account() call
            account_name_sql = text("SELECT name FROM account WHERE id = :takp_account_id")
            account_name_result = takp_conn.execute(account_name_sql.bindparams(takp_account_id=self.new_account_id))
            account_name_row = account_name_result.fetchone()

            if not account_name_row:
                raise Exception(f"Could not find TAKP account with ID {self.new_account_id}")

            account_name = account_name_row[0]

        # Now look up that account name in PEQ to get the PEQ account ID
        with self.eqemu_engine.connect() as peq_conn:
            peq_account_sql = text("SELECT id FROM account WHERE name = :account_name")
            peq_account_result = peq_conn.execute(peq_account_sql.bindparams(account_name=account_name))
            peq_account_row = peq_account_result.fetchone()

            if not peq_account_row:
                raise Exception(
                    f"Could not find PEQ account for account name '{account_name}'. Account should have been created by copy_account().")

            peq_account_id = peq_account_row[0]
            print(f"Mapped TAKP account {self.new_account_id} ('{account_name}') -> PEQ account {peq_account_id}")

            # If character existed before, reuse its ID. Otherwise let auto-increment assign one.
            if self.existing_peq_char_id:
                include_id_in_insert = True
                self.new_peq_char_id = self.existing_peq_char_id
                print(f"Reusing existing PEQ character ID: {self.new_peq_char_id}")
            else:
                include_id_in_insert = False

            if include_id_in_insert:
                insert_sql = text("""
                                  INSERT INTO character_data (`id`, `account_id`, `name`, `last_name`, `title`,
                                                              `suffix`,
                                                              `zone_id`, `zone_instance`, `y`, `x`, `z`, `heading`,
                                                              `gender`, `race`, `class`, `level`, `deity`, `birthday`,
                                                              `last_login`, `time_played`, `level2`, `anon`, `gm`,
                                                              `face`, `hair_color`, `hair_style`, `beard`,
                                                              `beard_color`,
                                                              `eye_color_1`, `eye_color_2`,
                                                              `drakkin_heritage`, `drakkin_tattoo`, `drakkin_details`,
                                                              `ability_time_seconds`, `ability_number`,
                                                              `ability_time_minutes`, `ability_time_hours`,
                                                              `exp`, `exp_enabled`,
                                                              `aa_points_spent`, `aa_exp`, `aa_points`,
                                                              `group_leadership_exp`, `raid_leadership_exp`,
                                                              `group_leadership_points`, `raid_leadership_points`,
                                                              `points`, `cur_hp`, `mana`, `endurance`, `intoxication`,
                                                              `str`, `sta`, `cha`, `dex`, `int`, `agi`, `wis`,
                                                              `extra_haste`, `zone_change_count`, `toxicity`,
                                                              `hunger_level`, `thirst_level`, `ability_up`,
                                                              `ldon_points_guk`, `ldon_points_mir`, `ldon_points_mmc`,
                                                              `ldon_points_ruj`, `ldon_points_tak`,
                                                              `ldon_points_available`,
                                                              `tribute_time_remaining`, `career_tribute_points`,
                                                              `tribute_points`, `tribute_active`,
                                                              `pvp_status`, `pvp_kills`, `pvp_deaths`,
                                                              `pvp_current_points`, `pvp_career_points`,
                                                              `pvp_best_kill_streak`, `pvp_worst_death_streak`,
                                                              `pvp_current_kill_streak`,
                                                              `pvp2`, `pvp_type`,
                                                              `show_helm`, `group_auto_consent`, `raid_auto_consent`,
                                                              `guild_auto_consent`, `leadership_exp_on`,
                                                              `RestTimer`, `air_remaining`, `autosplit_enabled`,
                                                              `lfp`, `lfg`, `mailkey`, `xtargets`,
                                                              `first_login`, `ingame`,
                                                              `e_aa_effects`, `e_percent_to_aa`, `e_expended_aa_spent`,
                                                              `aa_points_spent_old`, `aa_points_old`,
                                                              `e_last_invsnapshot`,
                                                              `deleted_at`, `illusion_block`)
                                  VALUES (:id, :account_id, :name, :last_name, :title, :suffix,
                                          :zone_id, 0, :y, :x, :z, :heading,
                                          :gender, :race, :class, :level, :deity, :birthday,
                                          :last_login, :time_played, :level2, :anon, :gm,
                                          :face, :hair_color, :hair_style, :beard, :beard_color,
                                          :eye_color_1, :eye_color_2,
                                          0, 0, 0,
                                          0, 0, 0, 0,
                                          :exp, 1,
                                          :aa_points_spent, :aa_exp, :aa_points,
                                          0, 0, 0, 0,
                                          :points, :cur_hp, :mana, :endurance, :intoxication,
                                          :str, :sta, :cha, :dex, :int, :agi, :wis,
                                          0, :zone_change_count, 0,
                                          :hunger_level, :thirst_level, 0,
                                          0, 0, 0, 0, 0, 0,
                                          0, 0, 0, 0,
                                          :pvp_status, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                          :show_helm, 0, 0, 0, 0,
                                          0, :air_remaining, :autosplit_enabled,
                                          0, 0, :mailkey, 5,
                                          :first_login, 0,
                                          :e_aa_effects, :e_percent_to_aa, :e_expended_aa_spent,
                                          0, 0, 0,
                                          NULL, 0)
                                  """)
            else:
                insert_sql = text("""
                                  INSERT INTO character_data (`account_id`, `name`, `last_name`, `title`, `suffix`,
                                                              `zone_id`, `zone_instance`, `y`, `x`, `z`, `heading`,
                                                              `gender`, `race`, `class`, `level`, `deity`, `birthday`,
                                                              `last_login`, `time_played`, `level2`, `anon`, `gm`,
                                                              `face`, `hair_color`, `hair_style`, `beard`,
                                                              `beard_color`,
                                                              `eye_color_1`, `eye_color_2`,
                                                              `drakkin_heritage`, `drakkin_tattoo`, `drakkin_details`,
                                                              `ability_time_seconds`, `ability_number`,
                                                              `ability_time_minutes`, `ability_time_hours`,
                                                              `exp`, `exp_enabled`,
                                                              `aa_points_spent`, `aa_exp`, `aa_points`,
                                                              `group_leadership_exp`, `raid_leadership_exp`,
                                                              `group_leadership_points`, `raid_leadership_points`,
                                                              `points`, `cur_hp`, `mana`, `endurance`, `intoxication`,
                                                              `str`, `sta`, `cha`, `dex`, `int`, `agi`, `wis`,
                                                              `extra_haste`, `zone_change_count`, `toxicity`,
                                                              `hunger_level`, `thirst_level`, `ability_up`,
                                                              `ldon_points_guk`, `ldon_points_mir`, `ldon_points_mmc`,
                                                              `ldon_points_ruj`, `ldon_points_tak`,
                                                              `ldon_points_available`,
                                                              `tribute_time_remaining`, `career_tribute_points`,
                                                              `tribute_points`, `tribute_active`,
                                                              `pvp_status`, `pvp_kills`, `pvp_deaths`,
                                                              `pvp_current_points`, `pvp_career_points`,
                                                              `pvp_best_kill_streak`, `pvp_worst_death_streak`,
                                                              `pvp_current_kill_streak`,
                                                              `pvp2`, `pvp_type`,
                                                              `show_helm`, `group_auto_consent`, `raid_auto_consent`,
                                                              `guild_auto_consent`, `leadership_exp_on`,
                                                              `RestTimer`, `air_remaining`, `autosplit_enabled`,
                                                              `lfp`, `lfg`, `mailkey`, `xtargets`,
                                                              `first_login`, `ingame`,
                                                              `e_aa_effects`, `e_percent_to_aa`, `e_expended_aa_spent`,
                                                              `aa_points_spent_old`, `aa_points_old`,
                                                              `e_last_invsnapshot`,
                                                              `deleted_at`, `illusion_block`)
                                  VALUES (:account_id, :name, :last_name, :title, :suffix,
                                          :zone_id, 0, :y, :x, :z, :heading,
                                          :gender, :race, :class, :level, :deity, :birthday,
                                          :last_login, :time_played, :level2, :anon, :gm,
                                          :face, :hair_color, :hair_style, :beard, :beard_color,
                                          :eye_color_1, :eye_color_2,
                                          0, 0, 0,
                                          0, 0, 0, 0,
                                          :exp, 1,
                                          :aa_points_spent, :aa_exp, :aa_points,
                                          0, 0, 0, 0,
                                          :points, :cur_hp, :mana, :endurance, :intoxication,
                                          :str, :sta, :cha, :dex, :int, :agi, :wis,
                                          0, :zone_change_count, 0,
                                          :hunger_level, :thirst_level, 0,
                                          0, 0, 0, 0, 0, 0,
                                          0, 0, 0, 0,
                                          :pvp_status, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                                          :show_helm, 0, 0, 0, 0,
                                          0, :air_remaining, :autosplit_enabled,
                                          0, 0, :mailkey, 5,
                                          :first_login, 0,
                                          :e_aa_effects, :e_percent_to_aa, :e_expended_aa_spent,
                                          0, 0, 0,
                                          NULL, 0)
                                  """)

        with self.eqemu_engine.connect() as conn:
            for record in results:
                # noinspection PyProtectedMember
                row = record._mapping

                # Build bindparams dict
                bind_params = {
                    'account_id': peq_account_id,
                    'name': row['name'],
                    'last_name': row['last_name'],
                    'title': row['title'],
                    'suffix': row['suffix'],
                    'zone_id': row['zone_id'],
                    # zone_instance = 0 (hardcoded)
                    'y': row['y'],
                    'x': row['x'],
                    'z': row['z'],
                    'heading': row['heading'],
                    'gender': row['gender'],
                    'race': row['race'],
                    'class': row['class'],
                    'level': row['level'],
                    'deity': row['deity'],
                    'birthday': row['birthday'],
                    'last_login': row['last_login'],
                    'time_played': row['time_played'],
                    'level2': row['level2'],
                    'anon': row['anon'],
                    'gm': row['gm'],
                    'face': row['face'],
                    'hair_color': row['hair_color'],
                    'hair_style': row['hair_style'],
                    'beard': row['beard'],
                    'beard_color': row['beard_color'],
                    'eye_color_1': row['eye_color_1'],
                    'eye_color_2': row['eye_color_2'],
                    # drakkin fields = 0 (hardcoded)
                    # ability fields = 0 (hardcoded)
                    'exp': row['exp'],
                    # exp_enabled = 1 (hardcoded)
                    'aa_points_spent': row['aa_points_spent'],
                    'aa_exp': row['aa_exp'],
                    'aa_points': row['aa_points'],
                    # leadership fields = 0 (hardcoded)
                    'points': row['points'],
                    'cur_hp': row['cur_hp'],
                    'mana': row['mana'],
                    'endurance': row['endurance'],
                    'intoxication': row['intoxication'],
                    'str': row['str'],
                    'sta': row['sta'],
                    'cha': row['cha'],
                    'dex': row['dex'],
                    'int': row['int'],
                    'agi': row['agi'],
                    'wis': row['wis'],
                    # extra_haste = 0 (hardcoded)
                    'zone_change_count': row['zone_change_count'],
                    # toxicity = 0 (hardcoded)
                    'hunger_level': row['hunger_level'],
                    'thirst_level': row['thirst_level'],
                    # ability_up = 0 (hardcoded)
                    # ldon_points fields = 0 (hardcoded)
                    # tribute fields = 0 (hardcoded)
                    'pvp_status': row['pvp_status'],
                    # extended pvp fields = 0 (hardcoded)
                    'show_helm': row['showhelm'],
                    # consent fields = 0 (hardcoded)
                    # RestTimer = 0 (hardcoded)
                    'air_remaining': row['air_remaining'],
                    'autosplit_enabled': row['autosplit_enabled'],
                    # lfp, lfg = 0 (hardcoded)
                    'mailkey': row['mailkey'],
                    # xtargets = 5 (hardcoded)
                    'first_login': row['firstlogon'],
                    # ingame = 0 (hardcoded)
                    'e_aa_effects': row['e_aa_effects'],
                    'e_percent_to_aa': row['e_percent_to_aa'],
                    'e_expended_aa_spent': row['e_expended_aa_spent']
                    # aa legacy fields = 0 (hardcoded)
                    # deleted_at = NULL (hardcoded)
                    # illusion_block = 0 (hardcoded)
                }
                # Add id if reusing existing character
                if include_id_in_insert:
                    bind_params['id'] = self.new_peq_char_id

                insert_sql = insert_sql.bindparams(**bind_params)
                conn.execute(insert_sql)

                # If new character, get the auto-generated ID
                if not include_id_in_insert:
                    result = conn.execute(text("SELECT LAST_INSERT_ID()"))
                    self.new_peq_char_id = result.scalar()
                    print(f"Assigned new PEQ character ID: {self.new_peq_char_id}")

            conn.commit()

    def copy_character_faction_values(self):
        """
        Copies TAKP character_faction_values table columns to the PEQ faction_values table
        """
        # noinspection SqlResolve
        sql = text("SELECT * FROM character_faction_values WHERE id = :old_char_id")
        sql = sql.bindparams(old_char_id=self.old_char_id)
        with self.eqmacemu_engine.connect() as conn:
            results = conn.execute(sql)

        insert_sql = text("INSERT INTO faction_values (`char_id`, `faction_id`, `current_value`, `temp`) "
                          "VALUES(:char_id, :faction_id, :current_value, :temp)")

        with self.eqemu_engine.connect() as conn:
            for record in results:
                # noinspection PyProtectedMember
                row = record._mapping
                insert_sql = insert_sql.bindparams(
                    char_id=self.new_peq_char_id,
                    faction_id=row['faction_id'],
                    current_value=row['current_value'],
                    temp=row['temp']
                )
                conn.execute(insert_sql)

            conn.commit()

    def copy_character_inventory(self):
        """
        Copies the character_inventory table from TAKP db to PEQ's inventory table
        with slot id translation

        TAKP → PEQ field mappings:
        - id → character_id
        - slotid → slot_id
        - itemid → item_id
        - charges → charges (unchanged)

        PEQ expansion fields set to 0:
        - color, augment_one through augment_six, instnodrop
        - ornament_icon, ornament_idfile, ornament_hero_model, guid

        TAKP fields lost: serialnumber, initialserial

        CRITICAL SLOT TRANSLATIONS:
        - Cursor: 0 → 33
        - Equipment: shifts slightly (Ammo 21→22)
        - General: 22-29 → 23-30
        - General bags: 250-329 → 262-341
        - Cursor bag: 330-339 → 342-351
        - Bank bags: 2030-2109 → 6210-7809
        """
        # noinspection SqlResolve
        sql = text("SELECT * FROM character_inventory WHERE id = :old_char_id")
        sql = sql.bindparams(old_char_id=self.old_char_id)
        with self.eqmacemu_engine.connect() as eqmac_conn:
            results = eqmac_conn.execute(sql)

        insert_sql = text("""
                          INSERT INTO inventory (character_id, slot_id, item_id, charges,
                                                 color, augment_one, augment_two, augment_three,
                                                 augment_four, augment_five, augment_six,
                                                 instnodrop, custom_data, ornament_icon,
                                                 ornament_idfile, ornament_hero_model, guid)
                          VALUES (:character_id, :slot_id, :item_id, :charges,
                                  0, 0, 0, 0, 0, 0, 0,
                                  0, :custom_data, 0, 0, 0, 0)
                          """)

        with self.eqemu_engine.connect() as eqemu_conn:
            for record in results:
                # noinspection PyProtectedMember
                row = record._mapping

                peq_slot = translate_slot_id_takp_to_peq(row['slotid'])

                insert_sql = insert_sql.bindparams(
                    character_id=self.new_peq_char_id,
                    slot_id=peq_slot,
                    item_id=row['itemid'],
                    charges=row['charges'],
                    custom_data=row['custom_data']
                )
                eqemu_conn.execute(insert_sql)

            eqemu_conn.commit()

    def copy_character_languages(self):
        """
        Copies the TAKP character_languages table to the PEQ character_languages table
        """
        sql = text("SELECT * FROM character_languages WHERE id = :old_char_id")
        sql = sql.bindparams(old_char_id=self.old_char_id)
        with self.eqmacemu_engine.connect() as conn:
            results = conn.execute(sql)

        insert_sql = text("INSERT INTO character_languages(id, lang_id, value) "
                          "VALUES (:id, :lang_id, :value)")

        with self.eqemu_engine.connect() as conn:
            for record in results:
                # noinspection PyProtectedMember
                row = record._mapping
                insert_sql = insert_sql.bindparams(
                    id=self.new_peq_char_id,
                    lang_id=row['lang_id'],
                    value=row['value']
                )
                conn.execute(insert_sql)

            conn.commit()

    def copy_character_keyring(self):
        """
        Copies the TAKP character_keyring table to the PEQ character_keyring table
        """
        # noinspection SqlResolve
        sql = text("SELECT * FROM character_keyring WHERE id = :old_char_id")
        sql = sql.bindparams(old_char_id=self.old_char_id)
        with self.eqmacemu_engine.connect() as conn:
            results = conn.execute(sql)

        insert_sql = text("INSERT INTO keyring (char_id, item_id) "
                          "VALUES (:char_id, :item_id)")

        with self.eqemu_engine.connect() as conn:
            for record in results:
                # noinspection PyProtectedMember
                row = record._mapping
                insert_sql = insert_sql.bindparams(
                    # allow PEQ id PK to just autoincrement
                    char_id=self.new_peq_char_id,
                    item_id=row['item_id']
                )
                conn.execute(insert_sql)

            conn.commit()

    def copy_character_spells(self):
        """
        Copies the character_spells table columns from TAKP to PEQ databases
        """
        sql = text("SELECT * FROM character_spells WHERE id = :old_char_id")
        sql = sql.bindparams(old_char_id=self.old_char_id)
        with self.eqmacemu_engine.connect() as conn:
            results = conn.execute(sql)

        insert_sql = text("INSERT INTO character_spells (id, slot_id, spell_id)"
                          "VALUES (:id, :slot_id, :spell_id)")

        with self.eqemu_engine.connect() as conn:
            for record in results:
                # noinspection PyProtectedMember
                row = record._mapping
                insert_sql = insert_sql.bindparams(
                    id=self.new_peq_char_id,
                    slot_id=row['slot_id'],
                    spell_id=row['spell_id']
                )
                conn.execute(insert_sql)

            conn.commit()

    def copy_character_memmed_spells(self):
        """
        Copies the character_memmed_spells table columns from TAKP to PEQ databases
        """
        sql = text("SELECT * FROM character_memmed_spells WHERE id = :old_char_id")
        sql = sql.bindparams(old_char_id=self.old_char_id)
        with self.eqmacemu_engine.connect() as conn:
            results = conn.execute(sql)

        insert_sql = text("INSERT INTO character_memmed_spells(id, slot_id, spell_id) "
                          "VALUES (:id, :slot_id, :spell_id)")

        with self.eqemu_engine.connect() as conn:
            for record in results:
                # noinspection PyProtectedMember
                row = record._mapping
                insert_sql = insert_sql.bindparams(
                    id=self.new_peq_char_id,
                    slot_id=row['slot_id'],
                    spell_id=row['spell_id']
                )
                conn.execute(insert_sql)

            conn.commit()

    def copy_character_skills(self):
        """
        Copies the character_skills table columns from TAKP to PEQ databases
        """
        sql = text("SELECT * FROM character_skills WHERE id = :old_char_id")
        sql = sql.bindparams(old_char_id=self.old_char_id)
        with self.eqmacemu_engine.connect() as conn:
            results = conn.execute(sql)

        insert_sql = text("INSERT INTO character_skills (id, skill_id, value) "
                          "VALUES (:id, :skill_id, :value)")

        with self.eqemu_engine.connect() as conn:
            for record in results:
                # noinspection PyProtectedMember
                row = record._mapping
                insert_sql = insert_sql.bindparams(
                    id=self.new_peq_char_id,
                    skill_id=row['skill_id'],
                    value=row['value']
                )
                conn.execute(insert_sql)

            conn.commit()

    # maybe quest globals


def main():
    """Run the transfer tool and copy tables"""
    parser = argparse.ArgumentParser(description='TAKP to PEQ character transfer tool')
    parser.add_argument('-c', '--character')
    args = parser.parse_args()
    with CharacterTransferTool(args.character) as ctt:
        ctt.clear_character_from_peqdb()
        ctt.copy_account()
        ctt.copy_account_ip()
        ctt.copy_character_data()
        ctt.copy_character_alternate_abilities()
        ctt.copy_character_bind()
        ctt.copy_character_currency()
        ctt.copy_character_faction_values()
        ctt.copy_character_inventory()
        ctt.copy_character_languages()
        ctt.copy_character_keyring()
        ctt.copy_character_spells()
        ctt.copy_character_memmed_spells()
        ctt.copy_character_skills()


if __name__ == "__main__":
    main()
