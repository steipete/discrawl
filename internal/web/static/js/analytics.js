async function loadChart(canvasId, endpoint, chartType, options) {
    options = options || {};
    var canvas = document.getElementById(canvasId);
    if (!canvas) return;
    try {
        var resp = await fetch(endpoint);
        if (!resp.ok) return;
        var data = await resp.json();
        if (window._charts && window._charts[canvasId]) {
            window._charts[canvasId].data = data;
            window._charts[canvasId].update();
            return;
        }
        window._charts = window._charts || {};
        window._charts[canvasId] = new Chart(canvas, { type: chartType, data: data, options: options });
    } catch (e) {
        console.error('loadChart error', canvasId, e);
    }
}

async function loadHeatmap(canvasId, endpoint) {
    var canvas = document.getElementById(canvasId);
    if (!canvas) return;
    try {
        var resp = await fetch(endpoint);
        if (!resp.ok) return;
        var json = await resp.json();
        var data = {
            datasets: [{
                label: 'Activity',
                data: json.data || [],
                backgroundColor: function(ctx) {
                    var value = ctx.raw ? ctx.raw.v : 0;
                    var max = Math.max(...(json.data || []).map(p => p.v));
                    var alpha = max > 0 ? value / max : 0;
                    return 'rgba(88,101,242,' + (alpha * 0.8 + 0.2) + ')';
                },
                borderWidth: 1,
                borderColor: 'rgba(63,65,71,0.8)',
                width: function(ctx) { return ctx.chart.width / 25; },
                height: function(ctx) { return ctx.chart.height / 8; }
            }]
        };
        var options = {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { display: false }, tooltip: { callbacks: {
                title: function(ctx) {
                    var days = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
                    return days[ctx[0].raw.y] + ' ' + ctx[0].raw.x + ':00';
                },
                label: function(ctx) { return ctx.raw.v + ' messages'; }
            }}},
            scales: {
                x: { type: 'linear', min: 0, max: 23, ticks: { stepSize: 2 }, title: { display: true, text: 'Hour of Day' } },
                y: { type: 'linear', min: 0, max: 6, ticks: { stepSize: 1, callback: function(val) {
                    var days = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];
                    return days[val];
                }}, title: { display: true, text: 'Day of Week' } }
            }
        };
        if (window._charts && window._charts[canvasId]) {
            window._charts[canvasId].data = data;
            window._charts[canvasId].update();
            return;
        }
        window._charts = window._charts || {};
        window._charts[canvasId] = new Chart(canvas, { type: 'scatter', data: data, options: options });
    } catch (e) {
        console.error('loadHeatmap error', canvasId, e);
    }
}

async function loadOverviewCards(endpoint) {
    var container = document.getElementById('overview-cards');
    if (!container) return;
    try {
        var resp = await fetch(endpoint);
        if (!resp.ok) return;
        var stats = await resp.json();
        var html = '<div class="metric-card"><div class="metric-value">' + (stats.message_count || 0).toLocaleString() + '</div><div class="metric-label">Total Messages</div></div>';
        html += '<div class="metric-card"><div class="metric-value">' + (stats.member_count || 0).toLocaleString() + '</div><div class="metric-label">Members</div></div>';
        html += '<div class="metric-card"><div class="metric-value">' + (stats.channel_count || 0).toLocaleString() + '</div><div class="metric-label">Channels</div></div>';
        container.innerHTML = html;
    } catch (e) {
        console.error('loadOverviewCards error', e);
    }
}

function getDateParams() {
    var fromInput = document.getElementById('date-from');
    var toInput = document.getElementById('date-to');
    if (fromInput && toInput && fromInput.value && toInput.value) {
        return '&from=' + fromInput.value + '&to=' + toInput.value;
    }
    var days = (document.getElementById('days-filter') || {}).value || 30;
    return '?days=' + days;
}

function refreshCharts() {
    var guildID = (document.getElementById('guild-id') || {}).value;
    if (!guildID) return;
    var base = '/api/v1/g/' + guildID + '/stats';
    var params = getDateParams();
    loadChart('msg-volume', base + '/message-volume' + params, 'bar', { responsive: true, plugins: { legend: { display: false } } });
    loadHeatmap('activity-heatmap', base + '/activity-heatmap' + params);
    loadChart('top-members', base + '/top-members' + params, 'bar', { indexAxis: 'y', responsive: true, plugins: { legend: { display: false } } });
    loadChart('channel-activity', base + '/channel-activity' + params, 'bar', { indexAxis: 'y', responsive: true, plugins: { legend: { display: false } } });
    loadOverviewCards(base + '/overview');
}

function toggleCustomRange() {
    var picker = document.getElementById('custom-range-picker');
    var select = document.getElementById('days-filter');
    if (!picker) return;
    if (picker.style.display === 'none') {
        picker.style.display = 'flex';
        if (select) select.disabled = true;
    } else {
        picker.style.display = 'none';
        if (select) select.disabled = false;
    }
}

function applyCustomRange() {
    var fromInput = document.getElementById('date-from');
    var toInput = document.getElementById('date-to');
    if (!fromInput || !toInput || !fromInput.value || !toInput.value) {
        alert('Please select both start and end dates');
        return;
    }
    if (new Date(fromInput.value) > new Date(toInput.value)) {
        alert('Start date must be before end date');
        return;
    }
    refreshCharts();
}

document.addEventListener('DOMContentLoaded', refreshCharts);
document.addEventListener('chartRefresh', refreshCharts);
