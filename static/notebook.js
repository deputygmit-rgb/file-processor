(function(){
  const $fileList = $('#nb-file-list');
  const $preview = $('#nb-preview-content');
  const $chatArea = $('#nb-chat-area');
  const $query = $('#nb-query');
  let selectedFileId = null;

  function makeChatBubble(who, text) {
    return $("<div class='nb-chat-item'>").append(
      $(`<div class='who'>${who}</div>`),
      $(`<div class='msg'></div>`).text(text)
    );
  }

  // click file
  $fileList.on('click', '.nb-file-item', function(){
    $fileList.find('.nb-file-item').removeClass('active');
    $(this).addClass('active');
    selectedFileId = $(this).data('file-id');
    $('#nb-selected-file').text($(this).find('.nb-file-title').text());
    loadFilePreview(selectedFileId);
  });

  $('#nb-send').on('click', async function(){
    const q = $query.val().trim();
    if (!q) return;
    $chatArea.append(makeChatBubble('You', q));
    $query.val('');

    // optimistic placeholder
    const loading = makeChatBubble('Assistant', 'Thinking...');
    $chatArea.append(loading);
    $chatArea.scrollTop($chatArea.prop('scrollHeight'));

    try {
      // Try a backend endpoint if available; falls back to local echo
      const payload = { query: q, file_id: selectedFileId };
      const res = await fetch('/api/query', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(payload)});
      if (res.ok) {
        const j = await res.json();
        loading.find('.msg').text(j.answer || JSON.stringify(j));
      } else {
        loading.find('.msg').text('No server response; this is a local demo.');
      }
    } catch (e) {
      loading.find('.msg').text('Local demo: ' + (q.length>120 ? q.slice(0,120)+'...' : q));
    }

    $chatArea.scrollTop($chatArea.prop('scrollHeight'));
  });

  $('#nb-search').on('input', function(){
    const q = $(this).val().toLowerCase();
    $fileList.find('.nb-file-item').each(function(){
      const txt = $(this).find('.nb-file-title').text().toLowerCase();
      $(this).toggle(txt.includes(q));
    });
  });

  async function loadFilePreview(fileId){
    $preview.html('<div class="nb-preview-empty">Loading preview...</div>');
    // Try to detect file type from DOM or call an API
    try {
      // Attempt to fetch file details from server endpoint; fallback to pre-rendered DOM attributes
      const res = await fetch(`/api/file_doc/${fileId}`);
      if (res.ok) {
        const doc = await res.json();
        renderPreviewFromDoc(doc);
        return;
      }
    } catch (e) {
      // ignore and fallback
    }
    // fallback: show static message
    $preview.html('<div class="nb-preview-empty">Preview unavailable - wire `/api/file_doc/<id>` to return JSON document for richer preview.</div>');
  }

  function renderPreviewFromDoc(doc) {
    const t = (doc.filetype||'').toLowerCase();
    let html = `<div><strong>${doc.filename || 'Untitled'}</strong> <div class='text-muted small'>${doc.filetype || ''}</div></div>`;
    if (t === 'pdf' && doc.data && Array.isArray(doc.data.pages)) {
      doc.data.pages.forEach(p=>{
        html += `<hr/><div><strong>Page ${p.page_number}</strong>`;
        // render page text preserving paragraphs
        const pageText = p.text || p.page_text || '';
        if (pageText) html += `<div style="white-space:pre-wrap;margin-top:6px;">${escapeHtml(pageText)}</div>`;
        // render any structured tables found on the page
        if (p.tables && Array.isArray(p.tables) && p.tables.length>0) {
          p.tables.forEach((tbl, ti)=>{
            html += `<div style="margin-top:8px;"><strong>Table ${ti+1}</strong>` + renderHtmlTable(tbl) + `</div>`;
          });
        }
        // render image-level OCR texts if present
        if (p.image_texts && p.image_texts.length>0) {
          html += `<div style="margin-top:8px;"><strong>Image OCR</strong>`;
          p.image_texts.forEach(it => { html += `<div style="white-space:pre-wrap;">${escapeHtml(it)}</div>`; });
          html += `</div>`;
        }
        html += `</div>`;
      });
    } else if (t === 'pptx' && doc.data && Array.isArray(doc.data)) {
      doc.data.forEach((sl, idx)=>{
        html += `<hr/><div><strong>Slide ${sl.slide_number || sl.slide_index || idx+1}</strong>`;
        const slideText = sl.text || sl.slide_text || sl.title || '';
        if (slideText) html += `<div style="white-space:pre-wrap;margin-top:6px;">${escapeHtml(slideText)}</div>`;
        if (sl.tables && Array.isArray(sl.tables) && sl.tables.length>0) {
          sl.tables.forEach((tbl, ti)=>{
            html += `<div style="margin-top:8px;"><strong>Table ${ti+1}</strong>` + renderHtmlTable(tbl) + `</div>`;
          });
        }
        // images OCR
        if (sl.images && sl.images.length>0) {
          sl.images.forEach(img=>{
            if (img.ocr_text) html += `<div style="white-space:pre-wrap;margin-top:6px;">${escapeHtml(img.ocr_text)}</div>`;
          });
        }
        html += `</div>`;
      });
    } else if (doc.data && typeof doc.data === 'object') {
      // If this looks like Excel sheets (mapping of sheet->rows)
      const isSheets = Object.values(doc.data).every(v => Array.isArray(v));
      if (isSheets) {
        for (const [sheetName, rows] of Object.entries(doc.data)) {
          html += `<hr/><div><strong>Sheet: ${escapeHtml(sheetName)}</strong>`;
          if (Array.isArray(rows) && rows.length>0) {
            html += renderHtmlTable(rows);
          } else {
            html += `<div class="nb-preview-empty">No rows</div>`;
          }
          html += `</div>`;
        }
      } else {
        html += `<pre style="white-space:pre-wrap;">${escapeHtml(JSON.stringify(doc.data, null, 2))}</pre>`;
      }
    } else {
      html += `<div class='nb-preview-empty'>No structured preview available.</div>`;
    }
    $preview.html(html);
  }

  function renderHtmlTable(tbl) {
    // tbl is expected to be an array of rows (each row is array of cells)
    if (!Array.isArray(tbl)) return '';
    let out = '<table class="table table-sm table-bordered" style="margin-top:6px;"><tbody>';
    tbl.forEach(row=>{
      out += '<tr>';
      if (Array.isArray(row)) {
        row.forEach(cell=>{
          out += `<td>${escapeHtml(cell===null||cell===undefined? '': cell)}</td>`;
        });
      } else if (typeof row === 'object') {
        // object-like row -> render values
        Object.values(row).forEach(v=> out += `<td>${escapeHtml(v===null||v===undefined? '': v)}</td>`);
      }
      out += '</tr>';
    });
    out += '</tbody></table>';
    return out;
  }

  function escapeHtml(text) {
    if (!text) return '';
    return text.toString().replace(/[&<>"]/g, function(m){ return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[m]; });
  }

})();
