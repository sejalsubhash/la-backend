import ExcelJS from 'exceljs';
import { uploadToS3 } from './s3Service.js';

export async function generateAuditReport(jobId, results, failedDocuments) {
  const workbook = new ExcelJS.Workbook();
  workbook.creator = 'Legal Audit Platform';
  workbook.created = new Date();

  // Tab 1: Legal Risk Audit (Detail)
  const detailSheet = workbook.addWorksheet('Legal Risk Audit', {
    views: [{ state: 'frozen', xSplit: 0, ySplit: 1 }]
  });

  // Define columns
  detailSheet.columns = [
    { header: 'Appl_No / Loan_No', key: 'appl_no', width: 22 },
    { header: 'Borrower_Name', key: 'borrower_name', width: 30 },
    { header: 'Property_Address', key: 'property_address', width: 45 },
    { header: 'Property_Type', key: 'property_type', width: 15 },
    { header: 'State', key: 'state', width: 15 },
    { header: 'TSR_Date', key: 'tsr_date', width: 12 },
    { header: 'Ownership_Title_Chain_Status', key: 'ownership_title_chain_status', width: 30 },
    { header: 'Encumbrances_Adverse_Entries', key: 'encumbrances_adverse_entries', width: 35 },
    { header: 'Subsequent_Charges', key: 'subsequent_charges', width: 35 },
    { header: 'Prior_Charge_Subsisting', key: 'prior_charge_subsisting', width: 30 },
    { header: 'ROC_Charge_Flag', key: 'roc_charge_flag', width: 25 },
    { header: 'Litigation_LisPendens', key: 'litigation_lis_pendens', width: 30 },
    { header: 'Mutation_Status', key: 'mutation_status', width: 15 },
    { header: 'Revenue_Municipal_Dues', key: 'revenue_municipal_dues', width: 20 },
    { header: 'Land_Use_Zoning_Status', key: 'land_use_zoning_status', width: 25 },
    { header: 'Stamping_Registration_Issues', key: 'stamping_registration_issues', width: 25 },
    { header: 'Mortgage_Perfection_Issues', key: 'mortgage_perfection_issues', width: 25 },
    { header: 'Advocate_Adverse_Remarks', key: 'advocate_adverse_remarks', width: 30 },
    { header: 'Risk_Rating', key: 'risk_rating', width: 12 },
    { header: 'Enforceability_Decision', key: 'enforceability_decision', width: 25 },
    { header: 'Enforceability_Rationale', key: 'enforceability_rationale', width: 50 },
    { header: 'Recommended_Actions', key: 'recommended_actions', width: 50 },
    { header: 'Document_Name', key: 'document_name', width: 30 },
    { header: 'Confidence_Score', key: 'confidence_score', width: 15 },
    { header: 'Processed_At', key: 'processed_at', width: 20 }
  ];

  // Style header row
  const headerRow = detailSheet.getRow(1);
  headerRow.font = { bold: true, color: { argb: 'FFFFFFFF' } };
  headerRow.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF2F5496' } };
  headerRow.alignment = { horizontal: 'center', vertical: 'middle', wrapText: true };
  headerRow.height = 30;

  // Add data rows
  const riskColors = {
    'High': 'FFFF6B6B',
    'Medium': 'FFFFB347',
    'Low': 'FF90EE90',
    'Manual Review Required': 'FFCCCCCC'
  };

  results.forEach((result, index) => {
    const row = detailSheet.addRow({
      appl_no: result.appl_no || 'Unknown',
      borrower_name: result.borrower_name || 'Unknown',
      property_address: result.property_address || 'Unknown',
      property_type: result.property_type || 'Unknown',
      state: result.state || 'Unknown',
      tsr_date: result.tsr_date || 'Unknown',
      ownership_title_chain_status: result.ownership_title_chain_status || 'Unknown',
      encumbrances_adverse_entries: result.encumbrances_adverse_entries || 'Unknown',
      subsequent_charges: result.subsequent_charges || 'Unknown',
      prior_charge_subsisting: result.prior_charge_subsisting || 'Unknown',
      roc_charge_flag: result.roc_charge_flag || 'Unknown',
      litigation_lis_pendens: result.litigation_lis_pendens || 'Unknown',
      mutation_status: result.mutation_status || 'Unknown',
      revenue_municipal_dues: result.revenue_municipal_dues || 'Unknown',
      land_use_zoning_status: result.land_use_zoning_status || 'Unknown',
      stamping_registration_issues: result.stamping_registration_issues || 'Unknown',
      mortgage_perfection_issues: result.mortgage_perfection_issues || 'Unknown',
      advocate_adverse_remarks: result.advocate_adverse_remarks || 'Unknown',
      risk_rating: result.risk_rating || 'Unknown',
      enforceability_decision: result.enforceability_decision || 'Unknown',
      enforceability_rationale: result.enforceability_rationale || 'Unknown',
      recommended_actions: result.recommended_actions || 'Unknown',
      document_name: result.document_name || 'Unknown',
      confidence_score: result.confidence_score || 0,
      processed_at: result.processed_at || new Date().toISOString()
    });

    row.alignment = { vertical: 'top', wrapText: true };
    
    // Apply risk-based row coloring
    const riskRating = result.risk_rating || 'Unknown';
    const fillColor = riskColors[riskRating] || 'FFFFFFFF';
    row.eachCell((cell) => {
      cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: fillColor } };
      cell.border = {
        top: { style: 'thin', color: { argb: 'FFCCCCCC' } },
        bottom: { style: 'thin', color: { argb: 'FFCCCCCC' } },
        left: { style: 'thin', color: { argb: 'FFCCCCCC' } },
        right: { style: 'thin', color: { argb: 'FFCCCCCC' } }
      };
    });
  });

  // Add data validation for Risk Rating
  detailSheet.dataValidations.add('S2:S9999', {
    type: 'list',
    formulae: ['"High,Medium,Low,Manual Review Required"']
  });

  // Add data validation for Enforceability Decision
  detailSheet.dataValidations.add('T2:T9999', {
    type: 'list',
    formulae: ['"Enforceable,Enforceable with Conditions,Not Enforceable,Manual Review Required"']
  });

  // Enable auto-filter
  detailSheet.autoFilter = {
    from: { row: 1, column: 1 },
    to: { row: results.length + 1, column: 25 }
  };

  // Tab 2: Summary
  const summarySheet = workbook.addWorksheet('Summary');
  
  // Calculate statistics
  const stats = {
    total: results.length,
    high: results.filter(r => r.risk_rating === 'High').length,
    medium: results.filter(r => r.risk_rating === 'Medium').length,
    low: results.filter(r => r.risk_rating === 'Low').length,
    manualReview: results.filter(r => r.risk_rating === 'Manual Review Required').length,
    enforceable: results.filter(r => r.enforceability_decision === 'Enforceable').length,
    enforceableWithConditions: results.filter(r => r.enforceability_decision === 'Enforceable with Conditions').length,
    notEnforceable: results.filter(r => r.enforceability_decision === 'Not Enforceable').length,
    avgConfidence: results.length > 0 
      ? Math.round(results.reduce((sum, r) => sum + (r.confidence_score || 0), 0) / results.length) 
      : 0
  };

  // Summary content
  const summaryData = [
    ['LEGAL RISK AUDIT SUMMARY REPORT'],
    [''],
    ['Report Generated:', new Date().toISOString()],
    ['Job ID:', jobId],
    [''],
    ['DOCUMENT STATISTICS'],
    ['Total Documents Processed:', stats.total],
    ['Failed/Manual Review:', failedDocuments.length],
    ['Average Confidence Score:', `${stats.avgConfidence}%`],
    [''],
    ['RISK DISTRIBUTION'],
    ['High Risk:', stats.high, `${stats.total > 0 ? Math.round(stats.high/stats.total*100) : 0}%`],
    ['Medium Risk:', stats.medium, `${stats.total > 0 ? Math.round(stats.medium/stats.total*100) : 0}%`],
    ['Low Risk:', stats.low, `${stats.total > 0 ? Math.round(stats.low/stats.total*100) : 0}%`],
    ['Manual Review Required:', stats.manualReview, `${stats.total > 0 ? Math.round(stats.manualReview/stats.total*100) : 0}%`],
    [''],
    ['ENFORCEABILITY SUMMARY'],
    ['Enforceable:', stats.enforceable],
    ['Enforceable with Conditions:', stats.enforceableWithConditions],
    ['Not Enforceable:', stats.notEnforceable],
    [''],
    ['TOP 10 HIGH RISK CASES']
  ];

  // Add top 10 high risk cases
  const highRiskCases = results
    .filter(r => r.risk_rating === 'High')
    .slice(0, 10);

  if (highRiskCases.length > 0) {
    summaryData.push(['#', 'Borrower', 'Property', 'Rationale']);
    highRiskCases.forEach((c, i) => {
      summaryData.push([
        i + 1,
        c.borrower_name || 'Unknown',
        c.property_address?.substring(0, 50) + '...' || 'Unknown',
        c.enforceability_rationale?.substring(0, 100) + '...' || 'Unknown'
      ]);
    });
  } else {
    summaryData.push(['No high risk cases found']);
  }

  summaryData.push(['']);
  summaryData.push(['RECURRING THEMES']);
  
  // Analyze recurring themes
  const themes = analyzeRecurringThemes(results);
  themes.forEach(theme => {
    summaryData.push([theme.issue, `${theme.count} cases`, `${theme.percentage}%`]);
  });

  // Add failed documents section
  if (failedDocuments.length > 0) {
    summaryData.push(['']);
    summaryData.push(['MANUAL REVIEW REQUIRED']);
    summaryData.push(['Document Name', 'Failure Reason']);
    failedDocuments.forEach(doc => {
      summaryData.push([doc.name, doc.reason || 'Processing failed']);
    });
  }

  // Write summary data
  summaryData.forEach((row, index) => {
    const excelRow = summarySheet.addRow(row);
    if (index === 0) {
      excelRow.font = { bold: true, size: 16 };
    } else if (['DOCUMENT STATISTICS', 'RISK DISTRIBUTION', 'ENFORCEABILITY SUMMARY', 'TOP 10 HIGH RISK CASES', 'RECURRING THEMES', 'MANUAL REVIEW REQUIRED'].includes(row[0])) {
      excelRow.font = { bold: true, color: { argb: 'FF2F5496' } };
    }
  });

  summarySheet.columns = [
    { width: 35 },
    { width: 40 },
    { width: 15 },
    { width: 60 }
  ];

  // Generate buffer and upload to S3
  const buffer = await workbook.xlsx.writeBuffer();
  const s3Key = `jobs/${jobId}/output/Legal_Audit_Report.xlsx`;
  
  await uploadToS3(s3Key, buffer, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
  
  return s3Key;
}

function analyzeRecurringThemes(results) {
  const themes = {
    'Mutation Pending': 0,
    'ROC Charge Search Required': 0,
    'Subsequent Charges Found': 0,
    'Prior Charges Subsisting': 0,
    'Title Chain Issues': 0,
    'Stamp/Registration Issues': 0,
    'Mortgage Perfection Issues': 0,
    'Litigation/Attachments': 0,
    'Property Tax Arrears': 0,
    'NA Conversion Pending': 0
  };

  results.forEach(r => {
    if (r.mutation_status?.toLowerCase().includes('pending')) themes['Mutation Pending']++;
    if (r.roc_charge_flag?.toLowerCase().includes('unknown') || r.roc_charge_flag?.toLowerCase().includes('required')) themes['ROC Charge Search Required']++;
    if (r.subsequent_charges?.toLowerCase().includes('yes')) themes['Subsequent Charges Found']++;
    if (r.prior_charge_subsisting?.toLowerCase().includes('yes')) themes['Prior Charges Subsisting']++;
    if (r.ownership_title_chain_status?.toLowerCase().includes('break') || r.ownership_title_chain_status?.toLowerCase().includes('defect')) themes['Title Chain Issues']++;
    if (r.stamping_registration_issues?.toLowerCase().includes('yes')) themes['Stamp/Registration Issues']++;
    if (r.mortgage_perfection_issues?.toLowerCase().includes('yes')) themes['Mortgage Perfection Issues']++;
    if (r.litigation_lis_pendens?.toLowerCase().includes('yes')) themes['Litigation/Attachments']++;
    if (r.revenue_municipal_dues?.toLowerCase().includes('yes')) themes['Property Tax Arrears']++;
    if (r.land_use_zoning_status?.toLowerCase().includes('pending')) themes['NA Conversion Pending']++;
  });

  return Object.entries(themes)
    .filter(([_, count]) => count > 0)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 10)
    .map(([issue, count]) => ({
      issue,
      count,
      percentage: results.length > 0 ? Math.round(count / results.length * 100) : 0
    }));
}
