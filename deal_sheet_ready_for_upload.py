#!/usr/bin/env python3
"""
Deal Sheet Ready for Upload - All-in-One Script

This script processes the untouched source file, applies all cleaning, renaming, calculations, and formatting steps,
and outputs a ready-for-upload file with the correct headers and Agent_Card as the agent's email.
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime

def main():
    # File paths
    source_file = "Fee Splits for all Agents August 12th V2.csv"
    agent_ref_file = "Source Data/REAL Team Report Agent Names and Emails Reference.csv"
    output_file = f"Deal Sheet Ready for Upload {datetime.now().strftime('%Y-%m-%d')}.csv"

    print(f"=== Deal Sheet Ready for Upload Script ===\nSource: {source_file}\nReference: {agent_ref_file}\nOutput: {output_file}\n")

    # 1. Read the data (single header format)
    df = pd.read_csv(source_file)
    print(f"Loaded {len(df)} records from source file.")
    print(f"Columns: {list(df.columns)}")

    # 2. Exclude incomplete records (missing Agent Name or Unique Commission ID)
    agent_name_col = 'Agent name' if 'Agent name' in df.columns else 'Agent_Name'
    unique_id_col = 'Deal ID' if 'Deal ID' in df.columns else 'Unique Commission ID'
    
    if agent_name_col not in df.columns:
        raise Exception(f"Could not find Agent Name column. Available columns: {list(df.columns)}")
    if unique_id_col not in df.columns:
        raise Exception(f"Could not find Unique Commission ID column. Available columns: {list(df.columns)}")
    
    before = len(df)
    df = df.dropna(subset=[agent_name_col, unique_id_col])
    print(f"Removed {before - len(df)} incomplete records (missing Agent Name or Unique Commission ID). Remaining: {len(df)}")

    # 3. Remove duplicate Unique Commission IDs (keep first occurrence)
    # NOTE: We'll do this AFTER creating the actual Unique Commission IDs, not based on Deal ID
    print(f"Note: Duplicate removal will be done after creating Unique Commission IDs")

    # 4. Round all currency fields to 2 decimal places
    currency_fields = [
        'Fee Amount', 'Total for Agent', 'GCI',
        'Override Amount', 'Total To House', 'Estimated Total for Agent'
    ]
    for col in currency_fields:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            df[col] = df[col].round(2)

    # 5. Add calculated commission column (GCI) if not present
    if 'GCI' not in df.columns:
        fee_col = 'Fee Amount'
        pct_col = 'Split with House'
        split_col = 'Split between Agents'
        
        if all(col in df.columns for col in [fee_col, pct_col, split_col]):
            fee_amt = pd.Series(pd.to_numeric(df[fee_col], errors='coerce')).fillna(0)
            pct_deal = pd.Series(pd.to_numeric(df[pct_col], errors='coerce')).fillna(0)
            pct_comm = pd.Series(pd.to_numeric(df[split_col], errors='coerce')).fillna(0)
            df['GCI'] = (fee_amt * (pct_deal / 100) * (pct_comm / 100)).round(2)
            print("Added calculated GCI column")
        else:
            print("Warning: Could not calculate GCI - missing required columns")

    # 6. Add Legacy_Deal column set to "Yes" if not present
    if 'Legacy Deal' not in df.columns:
        df['Legacy Deal'] = 'Yes'
        print("Added Legacy Deal column")

    # 7. Change all "Sales" to "Sale" and all "Commercial Lease" to "Commercial"
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].str.replace('Sales', 'Sale', regex=False)
            df[col] = df[col].str.replace('Commercial Lease', 'Commercial', regex=False)

    # 8. Add Estimated_Total_for_Agent column (identical to Total for Agent)
    if 'Estimated Total for Agent' not in df.columns and 'Total for Agent' in df.columns:
        df['Estimated Total for Agent'] = df['Total for Agent']
        print("Added Estimated Total for Agent column")

    # 8.5. Add Total to House from Agent column (house profit from this specific agent)
    if 'Total to House from Agent' not in df.columns:
        fee_col = 'Fee Amount'
        pct_col = 'Split with House'
        split_col = 'Split between Agents'
        
        if all(col in df.columns for col in [fee_col, pct_col, split_col]):
            # Check if we have the source "Total to House by Agent" column
            if 'Total to House by Agent' in df.columns:
                # Use the source values directly for maximum accuracy
                df['Total to House from Agent'] = pd.Series(pd.to_numeric(df['Total to House by Agent'], errors='coerce')).fillna(0)
                print("Added Total to House from Agent column (using source values)")
            else:
                # Calculate using our formula as fallback
                # Total to House from Agent = ((Fee Amount - Deductions off the top - Other Deductions) × House's percentage × Agent's deal percentage) - Marketing Deductions - Override Amount
                # House's percentage = (100 - Unified Commission Split) since house gets the remainder
                # Deductions off the top reduce the base amount before commission calculations
                # Other Deductions (if any) are additional deductions that reduce the commissionable amount
                # Marketing Deductions are agent-specific and reduce what that agent contributes to the house
                # Override Amount also reduces what that agent contributes to the house
                fee_amt = pd.Series(pd.to_numeric(df[fee_col], errors='coerce')).fillna(0)
                deductions_off_top = pd.Series(pd.to_numeric(df['Deduction off the top'], errors='coerce')).fillna(0)
                other_deductions = pd.Series(pd.to_numeric(df['Other Deductions (If any)'], errors='coerce')).fillna(0)
                marketing_deductions = pd.Series(pd.to_numeric(df['Marketing Deductions (if any)'], errors='coerce')).fillna(0)
                pct_comm = pd.Series(pd.to_numeric(df[pct_col], errors='coerce')).fillna(0)
                pct_deal = pd.Series(pd.to_numeric(df[split_col], errors='coerce')).fillna(0)
                override_amt = pd.Series(pd.to_numeric(df['Override Amount'], errors='coerce')).fillna(0)
                
                # Calculate net amount after all deal-level deductions
                net_amount = fee_amt - deductions_off_top - other_deductions
                base_house_profit = net_amount * ((100 - pct_comm) / 100) * (pct_deal / 100)
                # Subtract agent-specific deductions
                df['Total to House from Agent'] = (base_house_profit - marketing_deductions - override_amt).round(2)
                print("Added Total to House from Agent column (calculated using formula)")
        else:
            df['Total to House from Agent'] = 0
            print("Warning: Could not calculate Total to House from Agent - missing required columns")

    # 8.6. Add Unified Deal Close Date column (copy from Deal Date)
    if 'Unified Deal Close Date' not in df.columns:
        deal_date_col = 'Deal Date'
        if deal_date_col in df.columns:
            df['Unified Deal Close Date'] = df[deal_date_col]
            print(f"Added Unified Deal Close Date column (copied from {deal_date_col})")
        else:
            df['Unified Deal Close Date'] = ''
            print("Warning: Deal Date column not found - Unified Deal Close Date will be empty")

    # 9. Set Agent_Card column to agent's email using the reference CSV
    if 'Agent Card' not in df.columns:
        df['Agent Card'] = ''
    
    agent_ref = pd.read_csv(agent_ref_file)
    # Try to find the right columns in the reference file
    name_col = [col for col in agent_ref.columns if 'name' in col.lower()][0]
    email_col = [col for col in agent_ref.columns if 'email' in col.lower()][0]
    name_to_email = dict(zip(agent_ref[name_col].astype(str).str.strip(), agent_ref[email_col].astype(str).str.strip()))
    df['Agent Card'] = df[agent_name_col].astype(str).str.strip().map(name_to_email).fillna('')
    print(f"Mapped {len(df[df['Agent Card'] != ''])} agent emails")

    # --- New: Use target field names and order from example file ---
    target_header_file = "Deal Split Report Target Field Names Example.csv"
    with open(target_header_file, 'r') as f:
        target_header = f.readline().strip().split(',')
    
    # Replace the blank ID field with Numeric Deal ID - Legacy
    if 'ID' in target_header:
        id_index = target_header.index('ID')
        target_header[id_index] = 'Numeric Deal ID - Legacy'
    
    # Add Split_ID_Legacy field to the target header (after Numeric Deal ID - Legacy field)
    if 'Numeric Deal ID - Legacy' in target_header:
        deal_id_index = target_header.index('Numeric Deal ID - Legacy')
        target_header.insert(deal_id_index + 1, 'Split_ID_Legacy')
    
    # Add Total to House from Agent field to the target header (after Total to House field)
    if 'Total to House' in target_header:
        total_house_index = target_header.index('Total to House')
        target_header.insert(total_house_index + 1, 'Total to House from Agent')
    
    # Add Unified Deal Close Date field to the target header (after Deal Date - Legacy field)
    if 'Deal Date - Legacy' in target_header:
        deal_date_index = target_header.index('Deal Date - Legacy')
        target_header.insert(deal_date_index + 1, 'Unified Deal Close Date')
    
    # Note: Deal ID - Legacy is already in the header (replaced the blank ID field)
    
    # Add Discrepancy field to the target header (after Total to House from Agent field)
    if 'Total to House from Agent' in target_header:
        total_house_agent_index = target_header.index('Total to House from Agent')
        target_header.insert(total_house_agent_index + 1, 'Discrepancy')

    # Mapping from your processed columns to target field names
    # (add to this as needed for your data)
    col_map = {
        'Agent Card': 'Agent Card',
        'Legacy Deal': 'Legacy Deal',
        'Estimated Total for Agent': 'Estimated Total for Agent',
        'GCI': 'GCI',
        'Total for Agent': 'Total for Agent',
        'Fee Amount': 'Fee Amount - Legacy',
        'Split between Agents': 'Percent of Deal',  # FIXED: this is Percent of Deal
        'Split with House': 'Unified Commission Split',  # FIXED: this is Unified Commission Split
        'Agent name': 'Agent Name - Legacy',
        'Building ': 'Building Name -Legacy',
        'Deal Date': 'Deal Date - Legacy',
        'Unified Deal Close Date': 'Unified Deal Close Date',  # Added: Copy from Deal Close Date
        'Total To House': 'Total to House',
        'Total to House from Agent': 'Total to House from Agent',  # Added: House profit from specific agent
        'Split ID': 'Split_ID_Legacy',  # Added: Split ID maps to Split_ID_Legacy field
        'Override %': 'Override Percent',  # Added: Override % maps to Override Percent
        'Override Amount': 'Override Amount',  # Added: Override Amount maps to Override Amount
        'Status': 'Status - Legacy',  # Added: Status maps to Status - Legacy
        'Deal Type': 'Deal Type - Legacy',  # Added: Deal Type maps to Deal Type - Legacy
        'Deal ID': 'Numeric Deal ID - Legacy',  # Added: Deal ID maps to Numeric Deal ID - Legacy
        # Add more mappings as needed
    }

    # Build Unique Commission ID - Legacy in the correct format: {agent_email}-{deal_id}
    if 'Agent Card' in df.columns and 'Deal ID' in df.columns:
        df['Unique Commission ID - Legacy'] = df['Agent Card'].astype(str) + '-' + df['Deal ID'].astype(str)
    else:
        df['Unique Commission ID - Legacy'] = ''

    # Now remove duplicate Unique Commission IDs (this is the correct logic)
    before = len(df)
    df = df.drop_duplicates(subset=['Unique Commission ID - Legacy'], keep='first')
    print(f"Removed {before - len(df)} duplicate Unique Commission ID - Legacy values. Remaining: {len(df)}")

    # Build output DataFrame with correct columns and order
    output = pd.DataFrame()
    for col in target_header:
        # Find your column that maps to this target field
        if col == 'Unique Commission ID - Legacy':
            output[col] = df['Unique Commission ID - Legacy']
        # Note: Numeric Deal ID - Legacy is now handled by the column mapping
        else:
            src_col = next((k for k, v in col_map.items() if v == col), None)
            if src_col and src_col in df.columns:
                output[col] = df[src_col]
            else:
                output[col] = ''  # blank if not present
    
    # Calculate Discrepancy column
    # Discrepancy = (Sum of Total to House from Agent for the deal) - Total to House
    output['Discrepancy'] = 0.0  # Initialize
    
    # Group by Deal ID to calculate discrepancies
    for deal_id in output['Numeric Deal ID - Legacy'].unique():
        if deal_id:  # Skip empty values
            deal_mask = output['Numeric Deal ID - Legacy'] == deal_id
            deal_records = output[deal_mask]
            
            if len(deal_records) > 0:
                # Get the Total to House value (should be same for all records in the deal)
                total_to_house = deal_records['Total to House'].iloc[0]
                
                # Sum up all Total to House from Agent values for this deal
                sum_from_agents = deal_records['Total to House from Agent'].sum()
                
                # Calculate discrepancy
                discrepancy = sum_from_agents - total_to_house
                
                # Apply to all records in this deal
                output.loc[deal_mask, 'Discrepancy'] = discrepancy
    
    print("Added Discrepancy column (difference between calculated sum and source Total to House)")

    output.to_csv(output_file, index=False)
    print(f"\n✅ Output file created with target field names and order: {output_file} ({len(output)} records)")

if __name__ == "__main__":
    main() 