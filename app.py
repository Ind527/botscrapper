import streamlit as st
import pandas as pd
import time
from datetime import datetime
import os
from turbo_scraper import TurboTurmericBuyerScraper
from data_processor import DataProcessor
from utils import export_to_csv, validate_url

# Set page configuration
st.set_page_config(
    page_title="Turmeric Buyer Data Scraper",
    page_icon="🌿",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state
if 'scraped_data' not in st.session_state:
    st.session_state.scraped_data = pd.DataFrame()
if 'scraping_in_progress' not in st.session_state:
    st.session_state.scraping_in_progress = False

def main():
    st.title("🌿 Advanced Turmeric Buyer Intelligence Platform")
    st.markdown("### 🚀 10x Enhanced Real Company Data Extraction")
    st.info("💎 **Upgrade Complete**: Advanced scraping from 7+ real sources including MCA database, government directories, and international trade platforms")
    st.markdown("---")
    
    # Sidebar configuration
    with st.sidebar:
        st.header("⚙️ Configuration")
        
        # Target count slider
        target_count = st.slider(
            "Target number of companies:",
            min_value=10,
            max_value=100,
            value=50,
            step=5
        )
        
        # Turbo Speed Configuration
        st.subheader("⚡ Turbo Speed Settings")
        speed_mode = st.selectbox(
            "Speed Mode",
            ["🚀 Turbo (30x Faster)", "⚡ High Speed (10x Faster)", "🐌 Standard"],
            index=0
        )
        
        if speed_mode == "🚀 Turbo (30x Faster)":
            delay_seconds = 0.1
            st.success("⚡ Turbo mode: 30x faster scraping enabled!")
        elif speed_mode == "⚡ High Speed (10x Faster)":
            delay_seconds = 0.3
            st.info("⚡ High speed mode: 10x faster scraping")
        else:
            delay_seconds = 3
            st.warning("🐌 Standard mode: Normal speed scraping")
        
        # Search terms
        st.subheader("Search Terms")
        search_terms = st.text_area(
            "Enter search terms (one per line):",
            value="turmeric buyer\nturmeric importer\nspice buyer\nturmeric trader\nturmeric distributor",
            height=120
        )
        
        # Advanced Data Sources
        st.subheader("🔗 Advanced Data Sources")
        st.markdown("*Real company data from multiple verified sources*")
        
        # Primary Trade Platforms
        st.write("**Trade Platforms:**")
        use_tradeindia = st.checkbox("TradeIndia (Advanced Scraping)", value=True)
        use_indiamart = st.checkbox("IndiaMart (Multi-endpoint)", value=True)
        use_exportersindia = st.checkbox("ExportersIndia", value=True)
        
        # Company Registry Sources
        st.write("**Company Registries:**")
        use_zauba = st.checkbox("Zauba (MCA Database)", value=True, help="Ministry of Corporate Affairs registered companies")
        use_tofler = st.checkbox("Tofler (Business Intelligence)", value=True, help="Advanced company data and financials")
        
        # Government Sources
        st.write("**Government Sources:**")
        use_government = st.checkbox("Government Trade Directories", value=True, help="APEDA, DGFT and other official sources")
        
        # International Sources
        st.write("**International:**")
        use_alibaba = st.checkbox("Alibaba Buyers Directory", value=True, help="International turmeric buyers")
        
        # Clear data button
        if st.button("🗑️ Clear All Data", type="secondary"):
            st.session_state.scraped_data = pd.DataFrame()
            st.success("Data cleared successfully!")
            st.rerun()
    
    # Main content area
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("📊 Scraping Dashboard")
        
        # Current data summary
        if not st.session_state.scraped_data.empty:
            st.info(f"📈 Current dataset: {len(st.session_state.scraped_data)} companies")
        else:
            st.info("📭 No data collected yet")
        
        # Start scraping button
        if st.button(
            "🚀 Start Scraping",
            type="primary",
            disabled=st.session_state.scraping_in_progress,
            use_container_width=True
        ):
            start_scraping(target_count, delay_seconds, search_terms, use_tradeindia, use_indiamart, use_exportersindia, use_zauba, use_tofler, use_government, use_alibaba)
    
    with col2:
        st.subheader("📋 Quick Stats")
        if not st.session_state.scraped_data.empty:
            df = st.session_state.scraped_data
            
            # Display metrics
            col_a, col_b = st.columns(2)
            with col_a:
                st.metric("Total Companies", len(df))
                st.metric("With Email", len(df[df['email'].notna()]))
            with col_b:
                st.metric("With Phone", len(df[df['phone'].notna()]))
                st.metric("With Website", len(df[df['website'].notna()]))
    
    # Data display and management
    if not st.session_state.scraped_data.empty:
        st.markdown("---")
        display_data_section()
    
    # Export section
    if not st.session_state.scraped_data.empty:
        st.markdown("---")
        export_section()

def start_scraping(target_count, delay_seconds, search_terms, use_tradeindia, use_indiamart, use_exportersindia, use_zauba, use_tofler, use_government, use_alibaba):
    """Start the scraping process"""
    st.session_state.scraping_in_progress = True
    
    # Parse search terms
    terms_list = [term.strip() for term in search_terms.split('\n') if term.strip()]
    
    # Configure advanced sources
    sources = []
    if use_tradeindia:
        sources.append('tradeindia')
    if use_indiamart:
        sources.append('indiamart')
    if use_exportersindia:
        sources.append('exportersindia')
    if use_zauba:
        sources.append('zauba')
    if use_tofler:
        sources.append('tofler')
    if use_government:
        sources.append('government_data')
    if use_alibaba:
        sources.append('alibaba')
    
    if not sources:
        st.error("❌ Please select at least one source website!")
        st.session_state.scraping_in_progress = False
        return
    
    if not terms_list:
        st.error("❌ Please enter at least one search term!")
        st.session_state.scraping_in_progress = False
        return
    
    # Initialize turbo scraper for 30x faster speed
    scraper = TurboTurmericBuyerScraper(delay_seconds=delay_seconds)  # Ultra-fast scraping
    data_processor = DataProcessor()
    
    # Progress containers
    progress_container = st.container()
    
    with progress_container:
        st.subheader("🔄 Scraping in Progress")
        progress_bar = st.progress(0)
        status_text = st.empty()
        results_container = st.empty()
        
        try:
            # Start scraping
            collected_data = []
            total_collected = 0
            
            for i, term in enumerate(terms_list):
                if total_collected >= target_count:
                    break
                
                status_text.text(f"🔍 Searching for: {term}")
                
                # Search each source
                for source in sources:
                    if total_collected >= target_count:
                        break
                    
                    # Display advanced status with source details
                    source_names = {
                        'tradeindia': '🌐 TradeIndia (Advanced)',
                        'indiamart': '🏪 IndiaMart (Multi-endpoint)',
                        'exportersindia': '📋 ExportersIndia',
                        'zauba': '🏢 Zauba (MCA Database)',
                        'tofler': '📊 Tofler (Business Intel)',
                        'government_data': '🏛️ Government Sources',
                        'alibaba': '🌍 Alibaba International'
                    }
                    
                    status_text.text(f"{source_names.get(source, source)} • Searching: {term}")
                    
                    try:
                        # Advanced scraping from real sources
                        source_data = scraper.scrape_source(source, term, limit=target_count - total_collected)
                        
                        if source_data:
                            collected_data.extend(source_data)
                            total_collected = len(collected_data)
                            
                            # Update progress
                            progress = min(total_collected / target_count, 1.0)
                            progress_bar.progress(progress)
                            
                            status_text.text(f"✅ {source_names.get(source, source)}: Found {len(source_data)} real companies • Total: {total_collected}/{target_count}")
                            
                            # Show partial results
                            if collected_data:
                                df_temp = data_processor.process_data(collected_data)
                                results_container.dataframe(df_temp.tail(5), use_container_width=True)
                    
                    except Exception as e:
                        st.warning(f"⚠️ Error scraping {source} for {term}: {str(e)}")
                    
                    # Rate limiting
                    time.sleep(delay_seconds)
            
            # Process and save data
            if collected_data:
                processed_df = data_processor.process_data(collected_data)
                
                # Merge with existing data
                if not st.session_state.scraped_data.empty:
                    st.session_state.scraped_data = data_processor.merge_dataframes(
                        st.session_state.scraped_data, 
                        processed_df
                    )
                else:
                    st.session_state.scraped_data = processed_df
                
                status_text.text(f"✅ Scraping completed! Collected {len(collected_data)} companies.")
                progress_bar.progress(1.0)
                
                st.success(f"🎉 Successfully scraped {len(collected_data)} turmeric buyer companies!")
                
            else:
                st.warning("⚠️ No data was collected. Please try different search terms or sources.")
                
        except Exception as e:
            st.error(f"❌ An error occurred during scraping: {str(e)}")
        
        finally:
            st.session_state.scraping_in_progress = False
            time.sleep(2)
            st.rerun()

def display_data_section():
    """Display and manage scraped data"""
    st.subheader("📋 Collected Data")
    
    df = st.session_state.scraped_data
    
    # Search and filter options
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        search_query = st.text_input("🔍 Search companies:", placeholder="Enter company name, city, or keyword...")
    
    with col2:
        # City filter
        cities = ['All'] + sorted(df['city'].dropna().unique().tolist())
        selected_city = st.selectbox("🏙️ Filter by City:", cities)
    
    with col3:
        # Sort options
        sort_options = ['Company Name', 'City', 'Date Added']
        sort_by = st.selectbox("📊 Sort by:", sort_options)
    
    # Apply filters
    filtered_df = df.copy()
    
    if search_query:
        mask = (
            filtered_df['company_name'].str.contains(search_query, case=False, na=False) |
            filtered_df['city'].str.contains(search_query, case=False, na=False) |
            filtered_df['description'].str.contains(search_query, case=False, na=False)
        )
        filtered_df = filtered_df[mask]
    
    if selected_city != 'All':
        filtered_df = filtered_df[filtered_df['city'] == selected_city]
    
    # Sort data
    if sort_by == 'Company Name':
        filtered_df = filtered_df.sort_values('company_name')
    elif sort_by == 'City':
        filtered_df = filtered_df.sort_values('city')
    elif sort_by == 'Date Added':
        filtered_df = filtered_df.sort_values('date_added', ascending=False)
    
    # Display results count
    st.info(f"📊 Showing {len(filtered_df)} of {len(df)} companies")
    
    # Display data
    if not filtered_df.empty:
        # Column selection for display
        display_columns = st.multiselect(
            "Select columns to display:",
            options=filtered_df.columns.tolist(),
            default=['company_name', 'city', 'phone', 'email', 'website'],
            key="display_columns"
        )
        
        if display_columns:
            st.dataframe(
                filtered_df[display_columns],
                use_container_width=True,
                hide_index=True
            )
        else:
            st.warning("Please select at least one column to display.")
    else:
        st.warning("No data matches your search criteria.")

def export_section():
    """Handle data export functionality"""
    st.subheader("📤 Export Data")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # CSV Export
        if st.button("📊 Export as CSV", use_container_width=True):
            try:
                csv_data = export_to_csv(st.session_state.scraped_data)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"turmeric_buyers_{timestamp}.csv"
                
                st.download_button(
                    label="⬇️ Download CSV",
                    data=csv_data,
                    file_name=filename,
                    mime="text/csv",
                    use_container_width=True
                )
            except Exception as e:
                st.error(f"Error exporting CSV: {str(e)}")
    
    with col2:
        # Excel Export (using CSV for simplicity)
        if st.button("📈 Export as Excel", use_container_width=True):
            try:
                # Convert to Excel format
                from io import BytesIO
                output = BytesIO()
                st.session_state.scraped_data.to_excel(output, sheet_name='Turmeric Buyers', index=False, engine='openpyxl')
                
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"turmeric_buyers_{timestamp}.xlsx"
                
                st.download_button(
                    label="⬇️ Download Excel",
                    data=output.getvalue(),
                    file_name=filename,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
            except Exception as e:
                st.error(f"Error exporting Excel: {str(e)}")
    
    with col3:
        # JSON Export
        if st.button("🔗 Export as JSON", use_container_width=True):
            try:
                json_data = st.session_state.scraped_data.to_json(orient='records', indent=2)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"turmeric_buyers_{timestamp}.json"
                
                st.download_button(
                    label="⬇️ Download JSON",
                    data=json_data,
                    file_name=filename,
                    mime="application/json",
                    use_container_width=True
                )
            except Exception as e:
                st.error(f"Error exporting JSON: {str(e)}")

if __name__ == "__main__":
    main()
