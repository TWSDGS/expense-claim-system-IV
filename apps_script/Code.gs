const WEBAPP_API_CONFIG = {
  TIMEZONE: 'Asia/Taipei',
  HEADER_KEY_ROW: 1,
  HEADER_LABEL_ROW: 2,
  DATA_START_ROW: 3,

  SYSTEMS: {
    expense: {
      spreadsheetId: '1_l3O2VImO7vFhe1MZS0t4ktbHD4v53QSekj19BcRjBw',
      sheets: {
        submitted: '申請表單',
        draft: '草稿列表',
        users: 'Users',
        userDefaults: 'UserDefaults',
        options: 'Options',
        logs: '操作紀錄',
        settings: '系統設定',
      },
      formType: 'expense',
    },
    travel: {
      spreadsheetId: '1qC_4HAcKJPJ3vIAh_X9bZ8Fnw0DGYIr3NRGD669QK2Q',
      sheets: {
        submitted: '申請表單',
        draft: '草稿列表',
        users: 'Users',
        userDefaults: 'UserDefaults',
        options: 'Options',
        logs: '操作紀錄',
        settings: '系統設定',
      },
      formType: 'travel',
    },
  },
};

const SHEET_SCHEMAS = {
  expense_submitted: [
    ['record_id', '表單編號'], ['status', '狀態'], ['form_type', '表單類型'], ['form_date', '填寫日期'],
    ['plan_code', '計畫代號'], ['purpose_desc', '用途說明'],
    ['employee_enabled', '員工姓名_是否勾選'], ['employee_name', '員工姓名'], ['employee_no', '工號'],
    ['advance_offset_enabled', '借支沖抵_是否勾選'], ['advance_amount', '借支金額'], ['offset_amount', '沖銷金額'],
    ['balance_refund_amount', '餘額退回'], ['supplement_amount', '應補差額'],
    ['vendor_enabled', '逕付廠商_是否勾選'], ['vendor_name', '逕付廠商'], ['vendor_address', '地址'], ['vendor_payee_name', '收款人'],
    ['receipt_count', '憑證編號'], ['amount_untaxed', '未稅金額'], ['tax_mode', '稅額方式'], ['tax_amount', '稅額'], ['amount_total', '金額'],
    ['handler_name', '經辦人'], ['project_manager_name', '計畫主管'], ['department_manager_name', '部門主管'], ['accountant_name', '會計'],
    ['department', '部門'], ['note_public', '備註'], ['remarks_internal', '內部備註'],
    ['owner_name', '擁有人'], ['user_email', '使用者Email'], ['actor_role', '角色'], ['source_system', '來源系統'],
    ['created_at', '建立時間'], ['created_by', '建立者'], ['updated_at', '更新時間'], ['updated_by', '更新者'], ['submitted_at', '送出時間'], ['submitted_by', '送出者'],
    ['is_deleted', '是否刪除'], ['deleted_at', '刪除時間'], ['deleted_by', '刪除者'],
  ],

  expense_draft: [
    ['record_id', '表單編號'], ['status', '狀態'], ['form_type', '表單類型'], ['form_date', '填寫日期'],
    ['plan_code', '計畫代號'], ['purpose_desc', '用途說明'],
    ['employee_enabled', '員工姓名_是否勾選'], ['employee_name', '員工姓名'], ['employee_no', '工號'],
    ['advance_offset_enabled', '借支沖抵_是否勾選'], ['advance_amount', '借支金額'], ['offset_amount', '沖銷金額'],
    ['balance_refund_amount', '餘額退回'], ['supplement_amount', '應補差額'],
    ['vendor_enabled', '逕付廠商_是否勾選'], ['vendor_name', '逕付廠商'], ['vendor_address', '地址'], ['vendor_payee_name', '收款人'],
    ['receipt_count', '憑證編號'], ['amount_untaxed', '未稅金額'], ['tax_mode', '稅額方式'], ['tax_amount', '稅額'], ['amount_total', '金額'],
    ['handler_name', '經辦人'], ['project_manager_name', '計畫主管'], ['department_manager_name', '部門主管'], ['accountant_name', '會計'],
    ['department', '部門'], ['note_public', '備註'], ['remarks_internal', '內部備註'],
    ['owner_name', '擁有人'], ['user_email', '使用者Email'], ['actor_role', '角色'], ['source_system', '來源系統'],
    ['created_at', '建立時間'], ['created_by', '建立者'], ['updated_at', '更新時間'], ['updated_by', '更新者'], ['submitted_at', '送出時間'], ['submitted_by', '送出者'],
    ['is_deleted', '是否刪除'], ['deleted_at', '刪除時間'], ['deleted_by', '刪除者'],
  ],

  // travel schema：保留既有表頭，但在 sanitizeRecordForWrite_ 內完整接受前端別名
  travel_submitted: [
    ['record_id', '表單編號'], ['status', '狀態'], ['form_type', '表單類型'], ['form_date', '填寫日期'],
    ['employee_name', '出差人'], ['employee_no', '員工編號'], ['department', '部門'], ['plan_code', '計畫代號'],
    ['trip_purpose', '出差事由'], ['from_location', '出發地'], ['to_location', '目的地'],
    ['trip_date_start', '起始日期'], ['trip_time_start', '起始時間'], ['trip_date_end', '結束日期'], ['trip_time_end', '結束時間'], ['trip_days', '共天'],
    ['transport_tools', '交通方式'], ['transportation_type', '交通方式_字串'], ['gov_car_no', '公務車車號'], ['private_car_km', '私車公里數'], ['private_car_no', '私車車號'], ['other_transport_desc', '其他交通工具說明'],
    ['estimated_cost', '出差費預估'], ['expense_rows', '出差明細_JSON'], ['amount_total', '合計'], ['amount_total_upper', '總計新台幣'],
    ['attachments', '附件'], ['signature_file', '數位簽名檔'],
    ['handler_name', '出差人'], ['project_manager_name', '計畫主持人'], ['department_manager_name', '部門主管'], ['accountant_name', '管理處會計'],
    ['note_public', '備註'], ['remarks_internal', '內部備註'], ['send_pdf_to_email', '送出後寄送PDF到信箱'], ['budget_source', '預算來源'],
    ['owner_name', '擁有人'], ['user_email', '使用者Email'], ['actor_role', '角色'], ['source_system', '來源系統'],
    ['created_at', '建立時間'], ['created_by', '建立者'], ['updated_at', '更新時間'], ['updated_by', '更新者'], ['submitted_at', '送出時間'], ['submitted_by', '送出者'],
    ['is_deleted', '是否刪除'], ['deleted_at', '刪除時間'], ['deleted_by', '刪除者'],
  ],

  travel_draft: [
    ['record_id', '表單編號'], ['status', '狀態'], ['form_type', '表單類型'], ['form_date', '填寫日期'],
    ['employee_name', '出差人'], ['employee_no', '員工編號'], ['department', '部門'], ['plan_code', '計畫代號'],
    ['trip_purpose', '出差事由'], ['from_location', '出發地'], ['to_location', '目的地'],
    ['trip_date_start', '起始日期'], ['trip_time_start', '起始時間'], ['trip_date_end', '結束日期'], ['trip_time_end', '結束時間'], ['trip_days', '共天'],
    ['transport_tools', '交通方式'], ['transportation_type', '交通方式_字串'], ['gov_car_no', '公務車車號'], ['private_car_km', '私車公里數'], ['private_car_no', '私車車號'], ['other_transport_desc', '其他交通工具說明'],
    ['estimated_cost', '出差費預估'], ['expense_rows', '出差明細_JSON'], ['amount_total', '合計'], ['amount_total_upper', '總計新台幣'],
    ['attachments', '附件'], ['signature_file', '數位簽名檔'],
    ['handler_name', '出差人'], ['project_manager_name', '計畫主持人'], ['department_manager_name', '部門主管'], ['accountant_name', '管理處會計'],
    ['note_public', '備註'], ['remarks_internal', '內部備註'], ['send_pdf_to_email', '送出後寄送PDF到信箱'], ['budget_source', '預算來源'],
    ['owner_name', '擁有人'], ['user_email', '使用者Email'], ['actor_role', '角色'], ['source_system', '來源系統'],
    ['created_at', '建立時間'], ['created_by', '建立者'], ['updated_at', '更新時間'], ['updated_by', '更新者'], ['submitted_at', '送出時間'], ['submitted_by', '送出者'],
    ['is_deleted', '是否刪除'], ['deleted_at', '刪除時間'], ['deleted_by', '刪除者'],
  ],

  users: [
    ['name', '姓名'], ['email', 'Email'], ['role', '角色'], ['employee_no', '員工編號'], ['department', '部門'],
    ['is_active', '是否啟用'], ['sort_order', '排序'], ['can_view_all', '可看全部'], ['can_edit_all', '可編輯全部'], ['can_delete_all', '可刪除全部'], ['can_hard_delete', '可永久刪除'],
  ],

  user_defaults: [
    ['email', 'Email'], ['default_employee_name', '預設姓名'], ['default_employee_no', '預設員編'], ['default_department', '預設部門'], ['default_plan_code', '預設計畫代號'],
    ['default_handler_name', '預設經辦人/出差人'], ['default_project_manager_name', '預設計畫主管/主持人'], ['default_department_manager_name', '預設部門主管'], ['default_accountant_name', '預設會計/管理處會計'],
    ['default_note_public', '預設備註'], ['default_trip_time_start', '預設出差起始時間'], ['default_trip_time_end', '預設出差結束時間'], ['is_active', '是否啟用'],
  ],

  options: [
    ['option_type', '選項類型'], ['option_value', '選項值'], ['sort_order', '排序'], ['is_active', '是否啟用'], ['remark', '備註'],
  ],

  logs: [
    ['log_id', '紀錄編號'], ['record_id', '表單編號'], ['action', '動作'], ['actor_name', '執行者姓名'], ['actor_email', '執行者Email'], ['actor_role', '執行者角色'],
    ['target_status_before', '原狀態'], ['target_status_after', '新狀態'], ['action_time', '動作時間'], ['action_result', '結果'], ['message', '訊息'],
  ],

  settings: [
    ['setting_key', '設定鍵'], ['setting_value', '設定值'], ['remark', '備註'],
  ],
};

function setupAllSystems() { setupSystem_('expense'); setupSystem_('travel'); }
function setupExpenseSystem() { setupSystem_('expense'); }
function setupTravelSystem() { setupSystem_('travel'); }

function setupSystem_(systemKey) {
  const system = requireSystem_(systemKey);
  const ss = SpreadsheetApp.openById(system.spreadsheetId);
  if (systemKey === 'expense') {
    ensureSheetSchema_(ss, system.sheets.submitted, SHEET_SCHEMAS.expense_submitted);
    ensureSheetSchema_(ss, system.sheets.draft, SHEET_SCHEMAS.expense_draft);
  } else {
    ensureSheetSchema_(ss, system.sheets.submitted, SHEET_SCHEMAS.travel_submitted);
    ensureSheetSchema_(ss, system.sheets.draft, SHEET_SCHEMAS.travel_draft);
  }
  ensureSheetSchema_(ss, system.sheets.users, SHEET_SCHEMAS.users);
  ensureSheetSchema_(ss, system.sheets.userDefaults, SHEET_SCHEMAS.user_defaults);
  ensureSheetSchema_(ss, system.sheets.options, SHEET_SCHEMAS.options);
  ensureSheetSchema_(ss, system.sheets.logs, SHEET_SCHEMAS.logs);
  ensureSheetSchema_(ss, system.sheets.settings, SHEET_SCHEMAS.settings);
  seedDefaultData_(ss, systemKey);
}

function ensureSheetSchema_(ss, sheetName, schema) {
  let sheet = ss.getSheetByName(sheetName);
  if (!sheet) sheet = ss.insertSheet(sheetName);
  const keys = schema.map(r => r[0]);
  const labels = schema.map(r => r[1]);
  sheet.getRange(1, 1, 1, keys.length).setValues([keys]);
  sheet.getRange(2, 1, 1, labels.length).setValues([labels]);
  if (sheet.getFrozenRows() < 2) sheet.setFrozenRows(2);
  if (sheet.getMaxColumns() < keys.length) sheet.insertColumnsAfter(sheet.getMaxColumns(), keys.length - sheet.getMaxColumns());
}

function seedDefaultData_(ss, systemKey) {
  seedUsers_(ss.getSheetByName('Users'));
  seedUserDefaults_(ss.getSheetByName('UserDefaults'));
  seedOptions_(ss.getSheetByName('Options'), systemKey);
  seedSettings_(ss.getSheetByName('系統設定'), systemKey);
}

function seedUsers_(sheet) {
  if (sheet.getLastRow() >= 3) return;
  const rows = [
    ['Katherine', 'katherine@example.com', 'admin', 'A001', '化安處', true, 1, true, true, true, true],
    ['測試使用者', 'user@example.com', 'user', 'A002', '化安處', true, 2, false, false, false, false],
  ];
  sheet.getRange(3, 1, rows.length, rows[0].length).setValues(rows);
}

function seedUserDefaults_(sheet) {
  if (sheet.getLastRow() >= 3) return;
  const rows = [
    ['katherine@example.com', 'Katherine', 'A001', '化安處', 'TEST-001', 'Katherine', '主管A', '處長A', '會計A', '', '09:00', '17:00', true],
    ['user@example.com', '測試使用者', 'A002', '化安處', 'TEST-002', '測試使用者', '主管B', '處長B', '會計B', '', '09:00', '17:00', true],
  ];
  sheet.getRange(3, 1, rows.length, rows[0].length).setValues(rows);
}

function seedOptions_(sheet, systemKey) {
  if (sheet.getLastRow() >= 3) return;
  let rows = [
    ['employee_name', 'Katherine', 1, true, ''], ['employee_name', '測試使用者', 2, true, ''],
    ['employee_no', 'A001', 1, true, ''], ['employee_no', 'A002', 2, true, ''],
    ['department', '化安處', 1, true, ''], ['plan_code', 'TEST-001', 1, true, ''], ['plan_code', 'TEST-002', 2, true, ''],
  ];
  if (systemKey === 'expense') rows = rows.concat([['tax_mode', '5%', 1, true, ''], ['tax_mode', '免稅', 2, true, '']]);
  if (systemKey === 'travel') rows = rows.concat([
    ['from_location', '台南', 1, true, ''], ['from_location', '其他', 2, true, ''],
    ['to_location', '台北', 1, true, ''], ['to_location', '新北', 2, true, ''], ['to_location', '新竹', 3, true, ''], ['to_location', '台中', 4, true, ''], ['to_location', '台南', 5, true, ''], ['to_location', '高雄', 6, true, ''], ['to_location', '其他', 7, true, ''],
    ['vehicle_type', '高鐵', 1, true, ''], ['vehicle_type', '台鐵', 2, true, ''], ['vehicle_type', '客運', 3, true, ''], ['vehicle_type', '捷運', 4, true, ''], ['vehicle_type', '公車', 5, true, ''], ['vehicle_type', '計程車', 6, true, ''], ['vehicle_type', '私車公用', 7, true, ''], ['vehicle_type', '公務車', 8, true, ''], ['vehicle_type', '飛機', 9, true, ''], ['vehicle_type', '船舶', 10, true, ''], ['vehicle_type', '其他', 11, true, ''],
  ]);
  sheet.getRange(3, 1, rows.length, rows[0].length).setValues(rows);
}

function seedSettings_(sheet, systemKey) {
  if (sheet.getLastRow() >= 3) return;
  const rows = [['system_name', systemKey === 'expense' ? '支出憑證黏存單系統' : '國內出差申請單系統', '系統名稱'], ['version', 'v2026.03.travel-mapping-aligned', '版本']];
  sheet.getRange(3, 1, rows.length, rows[0].length).setValues(rows);
}

function doGet(e) {
  try {
    const params = e && e.parameter ? e.parameter : {};
    const action = (params.action || 'ping').trim();
    let result;
    switch (action) {
      case 'ping': result = ok_('pong', { server_time: nowIso_() }); break;
      case 'users_list': result = handleUsersList_(params); break;
      case 'user_defaults_list': result = handleUserDefaultsList_(params); break;
      case 'options_list': result = handleOptionsList_(params); break;
      case 'record_list_all': result = handleRecordListAll_(params); break;
      default: result = err_('unknown action: ' + action);
    }
    return jsonOutput_(result);
  } catch (error) {
    return jsonOutput_(err_(stringifyError_(error)));
  }
}

function doPost(e) {
  try {
    const body = parseJsonBody_(e);
    const action = ((body.action || '') + '').trim();
    let result;
    switch (action) {
      case 'record_save_draft': result = handleRecordSaveDraft_(body); break;
      case 'record_submit': result = handleRecordSubmit_(body); break;
      case 'record_soft_delete': result = handleRecordSoftDelete_(body); break;
      case 'record_hard_delete': result = handleRecordHardDelete_(body); break;
      default: result = err_('unknown action: ' + action);
    }
    return jsonOutput_(result);
  } catch (error) {
    return jsonOutput_(err_(stringifyError_(error)));
  }
}

function handleUsersList_(params) {
  const system = requireSystem_(params.system);
  const rows = readSheetObjects_(system, system.sheets.users)
    .filter(r => truthy_(r.is_active) || r.is_active === '' || r.is_active === undefined)
    .sort((a, b) => num_(a.sort_order) - num_(b.sort_order));
  return ok_('users loaded', { rows: rows, count: rows.length });
}

function handleUserDefaultsList_(params) {
  const system = requireSystem_(params.system);
  let rows = readSheetObjects_(system, system.sheets.userDefaults)
    .filter(r => truthy_(r.is_active) || r.is_active === '' || r.is_active === undefined);
  const email = normalizeEmail_(params.email || '');
  if (email) rows = rows.filter(r => normalizeEmail_(r.email) === email);
  return ok_('user defaults loaded', { rows: rows, count: rows.length });
}

function handleOptionsList_(params) {
  const system = requireSystem_(params.system);
  let rows = readSheetObjects_(system, system.sheets.options)
    .filter(r => truthy_(r.is_active) || r.is_active === '' || r.is_active === undefined)
    .sort((a, b) => num_(a.sort_order) - num_(b.sort_order));
  const optionType = (params.option_type || '').trim();
  if (optionType) rows = rows.filter(r => (r.option_type || '') === optionType);
  return ok_('options loaded', { rows: rows, count: rows.length });
}

function handleRecordListAll_(params) {
  const system = requireSystem_(params.system);
  const actor = buildActorFromParams_(params);
  let submittedRows = readSheetObjects_(system, system.sheets.submitted).map(r => { r._sheet_name = system.sheets.submitted; return r; });
  let draftRows = readSheetObjects_(system, system.sheets.draft).map(r => { r._sheet_name = system.sheets.draft; return r; });
  let allRows = submittedRows.concat(draftRows).filter(r => !truthy_(r.is_deleted));
  const status = ((params.status || '') + '').trim();
  if (status) allRows = allRows.filter(r => (r.status || '') === status);
  const ownerOnly = ((params.owner_only || '') + '').trim().toLowerCase() === 'true';
  if (ownerOnly && actor.email) allRows = allRows.filter(r => normalizeEmail_(r.user_email) === normalizeEmail_(actor.email));
  allRows.sort((a, b) => (b.updated_at || b.created_at || '').localeCompare(a.updated_at || a.created_at || ''));
  return ok_('records loaded', { rows: allRows, count: allRows.length });
}

function handleRecordSaveDraft_(body) {
  const system = requireSystem_(body.system);
  const actor = normalizeActor_(body.actor || {});
  const payload = body.payload || {};
  const record = sanitizeRecordForWrite_(system, payload, actor, 'draft');
  const lock = LockService.getScriptLock();
  lock.waitLock(30000);
  try {
    if (!record.record_id) record.record_id = generateRecordId_(system, record, actor);
    const existing = findRecordAnywhere_(system, record.record_id);
    if (existing) {
      record.created_at = existing.record.created_at || record.created_at;
      record.created_by = existing.record.created_by || record.created_by;
      upsertRecordToSheet_(system, system.sheets.draft, record, true);
      if (existing.sheetName !== system.sheets.draft) deleteRecordByIdFromSheet_(system, existing.sheetName, record.record_id);
    } else {
      upsertRecordToSheet_(system, system.sheets.draft, record, true);
    }
    return ok_('draft saved', { record_id: record.record_id, status: 'draft' });
  } finally { lock.releaseLock(); }
}

function handleRecordSubmit_(body) {
  const system = requireSystem_(body.system);
  const actor = normalizeActor_(body.actor || {});
  const payload = body.payload || {};
  const record = sanitizeRecordForWrite_(system, payload, actor, 'submitted');
  const lock = LockService.getScriptLock();
  lock.waitLock(30000);
  try {
    if (!record.record_id) record.record_id = generateRecordId_(system, record, actor);
    const existing = findRecordAnywhere_(system, record.record_id);
    if (existing) {
      record.created_at = existing.record.created_at || record.created_at;
      record.created_by = existing.record.created_by || record.created_by;
      upsertRecordToSheet_(system, system.sheets.submitted, record, true);
      if (existing.sheetName !== system.sheets.submitted) deleteRecordByIdFromSheet_(system, existing.sheetName, record.record_id);
    } else {
      upsertRecordToSheet_(system, system.sheets.submitted, record, true);
    }
    return ok_('record submitted', { record_id: record.record_id, status: 'submitted' });
  } finally { lock.releaseLock(); }
}

function handleRecordSoftDelete_(body) {
  const system = requireSystem_(body.system);
  const actor = normalizeActor_(body.actor || {});
  const recordId = ((body.payload || {}).record_id || '').trim();
  if (!recordId) return err_('record_id is required');
  const existing = findRecordAnywhere_(system, recordId);
  if (!existing) return err_('record not found');
  const record = Object.assign({}, existing.record, { status: 'deleted', is_deleted: true, deleted_at: nowIso_(), deleted_by: actor.email || '', updated_at: nowIso_(), updated_by: actor.email || '' });
  upsertRecordToSheet_(system, system.sheets.draft, record, true);
  if (existing.sheetName !== system.sheets.draft) deleteRecordByIdFromSheet_(system, existing.sheetName, recordId);
  return ok_('record soft deleted', { record_id: recordId });
}

function handleRecordHardDelete_(body) {
  const system = requireSystem_(body.system);
  const recordId = ((body.payload || {}).record_id || '').trim();
  if (!recordId) return err_('record_id is required');
  const existing = findRecordAnywhere_(system, recordId);
  if (!existing) return err_('record not found');
  deleteRecordByIdFromSheet_(system, existing.sheetName, recordId);
  return ok_('record hard deleted', { record_id: recordId });
}

function readSheetObjects_(system, sheetName) {
  const sheet = getSheet_(system, sheetName);
  const lastRow = sheet.getLastRow();
  const lastCol = sheet.getLastColumn();
  if (lastRow < WEBAPP_API_CONFIG.DATA_START_ROW || lastCol < 1) return [];
  const headers = sheet.getRange(WEBAPP_API_CONFIG.HEADER_KEY_ROW, 1, 1, lastCol).getValues()[0];
  const rowCount = lastRow - WEBAPP_API_CONFIG.DATA_START_ROW + 1;
  const values = sheet.getRange(WEBAPP_API_CONFIG.DATA_START_ROW, 1, rowCount, lastCol).getValues();
  return values.map((row, idx) => rowToObject_(headers, row, WEBAPP_API_CONFIG.DATA_START_ROW + idx)).filter(obj => !isEmptyRowObject_(obj, headers));
}

function upsertRecordToSheet_(system, sheetName, record, allowInsert) {
  const sheet = getSheet_(system, sheetName);
  const headers = getHeaderKeys_(sheet);
  const lastRow = sheet.getLastRow();
  let targetRow = null;
  if (lastRow >= WEBAPP_API_CONFIG.DATA_START_ROW) {
    const idCol = findHeaderIndex_(headers, 'record_id');
    if (idCol >= 0) {
      const idValues = sheet.getRange(WEBAPP_API_CONFIG.DATA_START_ROW, idCol + 1, lastRow - WEBAPP_API_CONFIG.DATA_START_ROW + 1, 1).getValues().flat();
      for (let i = 0; i < idValues.length; i++) {
        if (((idValues[i] || '') + '').trim() === record.record_id) { targetRow = WEBAPP_API_CONFIG.DATA_START_ROW + i; break; }
      }
    }
  }
  const rowValues = headers.map(h => record[h] !== undefined ? record[h] : '');
  if (targetRow) { sheet.getRange(targetRow, 1, 1, headers.length).setValues([rowValues]); return targetRow; }
  if (!allowInsert) throw new Error('record not found and insert not allowed');
  const insertRow = Math.max(sheet.getLastRow() + 1, WEBAPP_API_CONFIG.DATA_START_ROW);
  ensureSheetRows_(sheet, insertRow);
  sheet.getRange(insertRow, 1, 1, headers.length).setValues([rowValues]);
  return insertRow;
}

function deleteRecordByIdFromSheet_(system, sheetName, recordId) {
  const sheet = getSheet_(system, sheetName);
  const headers = getHeaderKeys_(sheet);
  const idCol = findHeaderIndex_(headers, 'record_id');
  if (idCol < 0) return false;
  const lastRow = sheet.getLastRow();
  if (lastRow < WEBAPP_API_CONFIG.DATA_START_ROW) return false;
  const idValues = sheet.getRange(WEBAPP_API_CONFIG.DATA_START_ROW, idCol + 1, lastRow - WEBAPP_API_CONFIG.DATA_START_ROW + 1, 1).getValues().flat();
  for (let i = idValues.length - 1; i >= 0; i--) {
    if (((idValues[i] || '') + '').trim() === recordId) { sheet.deleteRow(WEBAPP_API_CONFIG.DATA_START_ROW + i); return true; }
  }
  return false;
}

function findRecordAnywhere_(system, recordId) {
  const submitted = readSheetObjects_(system, system.sheets.submitted);
  const foundSubmitted = submitted.find(r => ((r.record_id || '') + '').trim() === recordId);
  if (foundSubmitted) return { sheetName: system.sheets.submitted, record: foundSubmitted };
  const draft = readSheetObjects_(system, system.sheets.draft);
  const foundDraft = draft.find(r => ((r.record_id || '') + '').trim() === recordId);
  if (foundDraft) return { sheetName: system.sheets.draft, record: foundDraft };
  return null;
}

function sanitizeRecordForWrite_(system, payload, actor, finalStatus) {
  const now = nowIso_();
  const clean = Object.assign({}, payload || {});

  clean.record_id = (clean.record_id || '').trim();
  clean.status = finalStatus;
  clean.form_type = system.formType;
  clean.owner_name = clean.owner_name || actor.name || '';
  clean.user_email = normalizeEmail_(clean.user_email || actor.email || '');
  clean.actor_role = actor.role || 'user';
  if (!clean.created_at) clean.created_at = now;
  if (!clean.created_by) clean.created_by = actor.email || '';
  clean.updated_at = now;
  clean.updated_by = actor.email || '';
  if (finalStatus === 'submitted') {
    clean.submitted_at = now;
    clean.submitted_by = actor.email || '';
    clean.is_deleted = false;
  }
  if (finalStatus === 'draft') clean.is_deleted = false;
  if (!clean.source_system) clean.source_system = system.formType;

  if (system.formType === 'expense') {
    clean.department = '化安處';
    clean.amount_untaxed = Math.round(Number(clean.amount_untaxed || 0));
    clean.tax_amount = Math.round(Number(clean.tax_amount || 0));
    clean.amount_total = Math.round(Number(clean.amount_total || 0));
    clean.receipt_count = Math.round(Number(clean.receipt_count || 0));
    clean.handler_name = '';
    clean.project_manager_name = '';
    clean.department_manager_name = '';
    clean.accountant_name = '';
    return clean;
  }

  // ===== travel alias mapping：前端欄位 -> 雲端 schema =====
  clean.employee_name = clean.employee_name || clean.traveler || clean.handler_name || '';
  clean.employee_no = clean.employee_no || actor.employee_no || '';
  clean.department = clean.department || actor.department || '';
  clean.plan_code = clean.plan_code || clean.project_id || '';
  clean.trip_purpose = clean.trip_purpose || clean.purpose || '';
  clean.from_location = clean.from_location || clean.departure_location || '';
  clean.to_location = clean.to_location || clean.destination_location || '';
  clean.trip_date_start = normalizeDateText_(clean.trip_date_start || clean.start_date || '');
  clean.trip_date_end = normalizeDateText_(clean.trip_date_end || clean.end_date || '');
  clean.trip_time_start = clean.trip_time_start || clean.start_time || '';
  clean.trip_time_end = clean.trip_time_end || clean.end_time || '';
  clean.budget_source = clean.budget_source || '';

  const transportList = normalizeTransportList_(clean.transport_tools || clean.transport_mode || clean.transport_options || clean.transportation_type || []);
  clean.transport_tools = JSON.stringify(transportList, null, 0);
  clean.transportation_type = transportList.join(',');
  clean.gov_car_no = clean.gov_car_no || clean.official_car_plate || '';
  clean.private_car_km = Number(clean.private_car_km || clean.private_mileage || 0) || 0;
  clean.private_car_no = clean.private_car_no || clean.private_car_plate || '';
  clean.other_transport_desc = clean.other_transport_desc || clean.other_transport_note || clean.other_transport || '';

  const rows = normalizeExpenseRows_(clean.expense_rows || clean.details || []);
  clean.expense_rows = JSON.stringify(rows, null, 0);
  clean.amount_total = Math.round(Number(clean.amount_total || sumExpenseRows_(rows)) || 0);
  clean.amount_total_upper = clean.amount_total_upper || numberToChineseMoney_(clean.amount_total);
  clean.estimated_cost = Math.round(Number(clean.estimated_cost || clean.amount_total || 0) || 0);

  clean.attachments = normalizeJsonText_(clean.attachments || clean.attachment_files || []);
  clean.signature_file = normalizeJsonText_(clean.signature_file || '');

  clean.handler_name = clean.handler_name || clean.employee_name || '';
  clean.project_manager_name = clean.project_manager_name || '';
  clean.department_manager_name = clean.department_manager_name || '';
  clean.accountant_name = clean.accountant_name || '';
  clean.note_public = clean.note_public || '';
  clean.remarks_internal = clean.remarks_internal || '';
  clean.send_pdf_to_email = truthy_(clean.send_pdf_to_email);
  clean.trip_days = calcTripDays_(clean.trip_date_start, clean.trip_date_end);

  return clean;
}

function normalizeDateText_(v) {
  if (!v) return '';
  const s = String(v).trim().replace(/\//g, '-');
  const m = s.match(/^(\d{4})-(\d{1,2})-(\d{1,2})$/);
  if (!m) return s;
  return m[1] + '-' + ('0' + m[2]).slice(-2) + '-' + ('0' + m[3]).slice(-2);
}

function normalizeTransportList_(value) {
  if (Array.isArray(value)) return value.map(x => String(x || '').trim()).filter(Boolean);
  const s = String(value || '').trim();
  if (!s) return [];
  try {
    const parsed = JSON.parse(s);
    if (Array.isArray(parsed)) return parsed.map(x => String(x || '').trim()).filter(Boolean);
  } catch (e) {}
  return s.split(',').map(x => x.trim()).filter(Boolean);
}

function normalizeExpenseRows_(value) {
  let rows = value;
  if (typeof rows === 'string') {
    try { rows = JSON.parse(rows); } catch (e) { rows = []; }
  }
  if (!Array.isArray(rows)) rows = [];
  return rows.map(r => ({
    '日期': normalizeDateText_(r['日期'] || r.date || ''),
    '起訖地點': r['起訖地點'] || r.route || '',
    '車別': r['車別'] || r.vehicle_type || '',
    '交通費': Number(r['交通費'] || r.transport_fee || 0) || 0,
    '膳雜費': Number(r['膳雜費'] || r.meal_fee || 0) || 0,
    '住宿費': Number(r['住宿費'] || r.lodging_fee || 0) || 0,
    '其它': Number(r['其它'] || r.other_fee || 0) || 0,
    '單據編號': r['單據編號'] || r.receipt_no || ''
  }));
}

function sumExpenseRows_(rows) {
  return (rows || []).reduce((acc, r) => acc + Number(r['交通費'] || 0) + Number(r['膳雜費'] || 0) + Number(r['住宿費'] || 0) + Number(r['其它'] || 0), 0);
}

function normalizeJsonText_(value) {
  if (value === '' || value === null || value === undefined) return '';
  if (typeof value === 'string') {
    const s = value.trim();
    if (!s) return '';
    try { JSON.parse(s); return s; } catch (e) { return JSON.stringify(value); }
  }
  return JSON.stringify(value);
}

function calcTripDays_(startDate, endDate) {
  try {
    const s = new Date(startDate); const e = new Date(endDate);
    if (isNaN(s.getTime()) || isNaN(e.getTime())) return '';
    const diff = Math.floor((e - s) / (1000 * 60 * 60 * 24)) + 1;
    return diff < 1 ? 1 : diff;
  } catch (e) { return ''; }
}

function numberToChineseMoney_(num) { if (num === '' || num === null || num === undefined) return ''; return String(num); }
function getSheet_(system, sheetName) { const ss = SpreadsheetApp.openById(system.spreadsheetId); const sheet = ss.getSheetByName(sheetName); if (!sheet) throw new Error('sheet not found: ' + sheetName); return sheet; }
function getHeaderKeys_(sheet) { const lastCol = sheet.getLastColumn(); return sheet.getRange(WEBAPP_API_CONFIG.HEADER_KEY_ROW, 1, 1, lastCol).getValues()[0]; }
function findHeaderIndex_(headers, key) { return headers.indexOf(key); }
function rowToObject_(headers, row, rowNumber) { const obj = {}; headers.forEach((h, i) => { obj[h] = row[i]; }); obj._row_number = rowNumber; return obj; }
function isEmptyRowObject_(obj, headers) { for (let i = 0; i < headers.length; i++) { const v = obj[headers[i]]; if (v !== '' && v !== null && v !== undefined) return false; } return true; }
function ensureSheetRows_(sheet, targetRow) { const maxRows = sheet.getMaxRows(); if (maxRows < targetRow) sheet.insertRowsAfter(maxRows, targetRow - maxRows); }
function parseJsonBody_(e) { if (!e || !e.postData || !e.postData.contents) throw new Error('missing post body'); return JSON.parse(e.postData.contents); }
function buildActorFromParams_(params) { return normalizeActor_({ name: params.actor_name || '', email: params.actor_email || '', role: params.actor_role || 'user' }); }
function normalizeActor_(actor) { return { name: (actor.name || '').trim(), email: normalizeEmail_(actor.email || ''), role: (actor.role || 'user').trim(), employee_no: actor.employee_no || '', department: actor.department || '' }; }
function requireSystem_(systemKey) { const key = ((systemKey || '') + '').trim().toLowerCase(); const system = WEBAPP_API_CONFIG.SYSTEMS[key]; if (!system) throw new Error('invalid system, expected expense or travel'); return system; }
function generateRecordId_(system, record, actor) {
  const employeeNo = String(record.employee_no || actor.employee_no || '00000').replace(/\D/g, '') || '00000';
  const rawDate = String(record.form_date || Utilities.formatDate(new Date(), WEBAPP_API_CONFIG.TIMEZONE, 'yyyy-MM-dd')).replace(/\//g, '-');
  const m = rawDate.match(/^(\d{4})-(\d{1,2})-(\d{1,2})$/);
  const yyyy = m ? Number(m[1]) : Number(Utilities.formatDate(new Date(), WEBAPP_API_CONFIG.TIMEZONE, 'yyyy'));
  const mm = m ? ('0' + m[2]).slice(-2) : Utilities.formatDate(new Date(), WEBAPP_API_CONFIG.TIMEZONE, 'MM');
  const dd = m ? ('0' + m[3]).slice(-2) : Utilities.formatDate(new Date(), WEBAPP_API_CONFIG.TIMEZONE, 'dd');
  const rocYmd = ('000' + (yyyy - 1911)).slice(-3) + mm + dd;
  const formPrefix = system.formType === 'travel' ? 'TR' : 'EX';
  const prefix = formPrefix + employeeNo + rocYmd;
  const existingIds = [].concat(readSheetObjects_(system, system.sheets.submitted).map(r => String(r.record_id || '').trim())).concat(readSheetObjects_(system, system.sheets.draft).map(r => String(r.record_id || '').trim()));
  let maxSeq = 0;
  existingIds.forEach(id => { if (id.indexOf(prefix) === 0) { const seq = parseInt(id.slice(prefix.length), 10); if (!isNaN(seq) && seq > maxSeq) maxSeq = seq; } });
  return prefix + ('000' + (maxSeq + 1)).slice(-3);
}
function normalizeEmail_(email) { return String(email || '').trim().toLowerCase(); }
function truthy_(v) { if (v === true) return true; const s = String(v || '').trim().toLowerCase(); return ['true', '1', 'yes', 'y'].indexOf(s) >= 0; }
function num_(v) { const n = Number(v); return isNaN(n) ? 999999 : n; }
function nowIso_() { return Utilities.formatDate(new Date(), WEBAPP_API_CONFIG.TIMEZONE, "yyyy-MM-dd'T'HH:mm:ss"); }
function ok_(message, data) { return { ok: true, message: message, data: data || {} }; }
function err_(message) { return { ok: false, message: message }; }
function jsonOutput_(obj) { return ContentService.createTextOutput(JSON.stringify(obj)).setMimeType(ContentService.MimeType.JSON); }
function stringifyError_(error) { return error.stack || error.message || String(error || 'unknown error'); }
