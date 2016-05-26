/**
 * Created by as56 on 25/05/2016.
 */

function reload_records() {

    var form_data = collect_form_data();
    form_data['format'] = 'json';

    $("#alert-box").text("Loading records...").removeClass("alert-success").addClass("alert-warning");

    $.ajax({
       url: '/api/events/',
       // ?order_by=date_time&page_number=1&recs_per_page=2&format=json',
       data: form_data,
       // {format: 'json'},
       error: function() {
          $('#alert-box').html('<p>Oops, could not reload records - please change the filters and try again.</p>');
       },
       dataType: 'json',
       success: function(data) {
           populate_table(data);
           //populate_form(data);
           //$('#alert-box').hide();
       },
       type: 'GET'
    });
}

function collect_form_data() {

    var form_data = {};
    var fields = ['dataset', 'process_stage', 'recs_per_page', 'order_by', 'withdrawn',
                  'succeeded', 'action_type'];

    for (var i=0; i < fields.length; i++) {
        var key = fields[i];
        form_data[key] = $("#" + key).val();
    }
    return form_data;
}

function populate_table(data) {
    // data is a list of dictionaries of content
    var html = '';

    for (var i=0; i < data.length; i++) {
        var record = data[i];
        var row = create_table_row(record);
        html += '<tr scope="row" style="font-size: 12px;">' + row + '<td>\n';
    }

    $("#table-body").html(html);
}

function create_table_row(content) {
    var row = '';
    var keys = ['date_time', 'dataset.name', 'process_stage.name', 'message', 'action_type', 'succeeded', 'withdrawn'];
    var font_color = 'black';

    if (content[succeeded]) {
        font_color = 'red';
    }

    var items = [content.date_time.replace('T', ' ').split('.')[0], content.dataset.name,
                 content.process_stage.name.replace('Controller', ''),
                 content.message, content.action_type, content.succeeded, content.dataset.is_withdrawn];

    for (var i=0; i < items.length; i++) {
        row += '<td color="' + font_color + '">' + items[i] + '</td>\n';
    }

    return row;
}


