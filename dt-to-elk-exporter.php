<?php
/**
 * Plugin Name: Disciple.Tools to ELK Exporter
 * Description: Exports Disciple.Tools data (contacts, groups, appointments, tasks) to ELK via Bulk API.
 * Version: 1.39
 * Author: Jon Ralls
 */

if (!defined('ABSPATH')) exit;

function dtelk_register_settings() {
    add_option('dtelk_elk_endpoint', '');
    add_option('dtelk_api_key', '');
    add_option('dtelk_index_name', '');
    add_option('dtelk_team_title', '');
    register_setting('dtelk_options_group', 'dtelk_elk_endpoint');
    register_setting('dtelk_options_group', 'dtelk_api_key');
    register_setting('dtelk_options_group', 'dtelk_index_name');
    register_setting('dtelk_options_group', 'dtelk_team_title');
}
add_action('admin_init', 'dtelk_register_settings');

function dtelk_add_admin_menu() {
    add_menu_page('Disciple.Tools to ELK Exporter', 'D.T. ELK Exporter', 'manage_options', 'dtelk-exporter', 'dtelk_exporter_admin_page', 'dashicons-database-export', 81);
}
add_action('admin_menu', 'dtelk_add_admin_menu');

function dtelk_exporter_admin_page() {
    ?>
    <div class="wrap">
        <h1>Disciple.Tools to ELK Exporter</h1>
        <p>Configure the settings below to connect to your Elasticsearch instance.</p>
        <form method="post" action="options.php">
            <?php settings_fields('dtelk_options_group'); ?>
            <table class="form-table">
                <tr valign="top">
                    <th scope="row">ELK Endpoint (/_bulk)</th>
                    <td><input type="text" name="dtelk_elk_endpoint" value="<?php echo esc_attr(get_option('dtelk_elk_endpoint')); ?>" size="60"></td>
                </tr>
                <tr valign="top">
                    <th scope="row">API Key</th>
                    <td><input type="text" name="dtelk_api_key" value="<?php echo esc_attr(get_option('dtelk_api_key')); ?>" size="60"></td>
                </tr>
                <tr valign="top">
                    <th scope="row">Index Name</th>
                    <td><input type="text" name="dtelk_index_name" value="<?php echo esc_attr(get_option('dtelk_index_name')); ?>" size="40"></td>
                </tr>
                <tr valign="top">
                    <th scope="row">Team Title</th>
                    <td><input type="text" name="dtelk_team_title" value="<?php echo esc_attr(get_option('dtelk_team_title')); ?>" size="40"></td>
                </tr>
            </table>
            <?php submit_button(); ?>
        </form>
        <hr>
        <h2>Manual Export</h2>
        <p>Click the button below to trigger a manual export of all data to ELK.</p>
        <form method="post">
            <?php wp_nonce_field('dtelk_manual_export_nonce', 'dtelk_manual_export_nonce_field'); ?>
            <?php submit_button('Manual Export to ELK', 'primary', 'dtelk_manual_export'); ?>
        </form>
    </div>
    <?php
}

function dtelk_handle_manual_export() {
    if (isset($_POST['dtelk_manual_export']) && check_admin_referer('dtelk_manual_export_nonce', 'dtelk_manual_export_nonce_field')) {
        $result = dtelk_export_to_elk();
        if (is_wp_error($result)) {
            $msg = $result->get_error_message();
            add_action('admin_notices', function() use ($msg) {
                echo "<div class='notice notice-error'><p>Export failed: " . esc_html($msg) . "</p></div>";
            });
        } else {
            add_action('admin_notices', function() {
                echo "<div class='notice notice-success is-dismissible'><p>Manual export to ELK completed successfully.</p></div>";
            });
        }
    }
}
add_action('admin_init', 'dtelk_handle_manual_export');

function dtelk_stringify_values($data) {
    if (is_array($data)) {
        return array_map('dtelk_stringify_values', $data);
    }
    return is_scalar($data) ? (string) $data : $data;
}

/**
 * Ensure the index exists with the required date field mappings.
 * Safe to call on every export — adding mappings to an existing index is a no-op for
 * fields that already exist, and ES rejects type changes rather than silently corrupting data.
 */
function dtelk_ensure_index_mappings($base_url, $api_key, $index) {
    $headers = [
        'Content-Type'  => 'application/json',
        'Authorization' => 'ApiKey ' . $api_key,
    ];
    $ssl_args = ['sslverify' => false, 'headers' => $headers, 'timeout' => 15];

    $index_url = rtrim($base_url, '/') . '/' . $index;

    // Check whether the index exists.
    $head = wp_remote_head($index_url, $ssl_args);
    $exists = !is_wp_error($head) && wp_remote_retrieve_response_code($head) === 200;

    $mappings = [
        'properties' => [
            'team_title'       => ['type' => 'keyword'],
            'date_created_ms'  => ['type' => 'date', 'format' => 'epoch_millis'],
            'date_modified_ms' => ['type' => 'date', 'format' => 'epoch_millis'],
            'meta'            => [
                'properties' => [
                    'first_contact_date_ms' => ['type' => 'date', 'format' => 'epoch_millis'],
                    'baptism_date_ms'       => ['type' => 'date', 'format' => 'epoch_millis'],
                    'start_date_ms'         => ['type' => 'date', 'format' => 'epoch_millis'],
                    'church_start_date_ms'  => ['type' => 'date', 'format' => 'epoch_millis'],
                ],
            ],
        ],
    ];

    if (!$exists) {
        // Create the index with mappings pre-applied so auto-mapping never runs first.
        wp_remote_request($index_url, array_merge($ssl_args, [
            'method' => 'PUT',
            'body'   => json_encode(['mappings' => $mappings]),
        ]));
    } else {
        // Index exists — add any missing fields (safe; ES ignores already-correct fields).
        wp_remote_request($index_url . '/_mapping', array_merge($ssl_args, [
            'method' => 'PUT',
            'body'   => json_encode($mappings),
        ]));
    }
}

function dtelk_export_to_elk() {
    $endpoint = rtrim(get_option('dtelk_elk_endpoint'), '/') . '/_bulk';
    $api_key = get_option('dtelk_api_key');
    $index = get_option('dtelk_index_name');

    if (!$endpoint || !$api_key || !$index) {
        return new WP_Error('missing_config', 'ELK settings (Endpoint, API Key, Index Name) are missing.');
    }

    // Derive the base ES URL (strip the /_bulk suffix added above).
    $base_url = substr($endpoint, 0, -strlen('/_bulk'));
    dtelk_ensure_index_mappings($base_url, $api_key, $index);

    $post_types = ['contacts', 'groups', 'dt_appointments', 'dt_tasks'];
    $lines = [];
    $ignored_meta_prefixes = ['contact'];
    $export_timestamp = gmdate('c');
    $team_title = get_option('dtelk_team_title');

    foreach ($post_types as $type) {
        $posts = get_posts([
            'post_type' => $type,
            'numberposts' => -1,
            'post_status' => 'any'
        ]);

        foreach ($posts as $post) {
            $meta = get_post_meta($post->ID);
            $flat_meta = [];
            $contact_address = null;
            foreach ($meta as $key => $value) {
                if (preg_match('/^contact_address_\w{3}$/', $key)) {
                    $contact_address = dtelk_stringify_values(maybe_unserialize($value[0]));
                }
                $skip = false;
                foreach ($ignored_meta_prefixes as $prefix) {
                    if (strpos($key, $prefix) === 0) {
                        $skip = true;
                        break;
                    }
                }
                if ($skip) continue;
                $flat_meta[$key] = dtelk_stringify_values(maybe_unserialize($value[0]));
            }
            if ($contact_address !== null) {
                $flat_meta['contact_address'] = $contact_address;
            }

            if (!empty($flat_meta['first_contact_date']) && is_numeric($flat_meta['first_contact_date'])) {
                $flat_meta['first_contact_date'] = gmdate('Y-m-d H:i:s', (int) $flat_meta['first_contact_date']);
            }
            if ($post->post_type === 'contacts' && empty($flat_meta['first_contact_date'])) {
                $flat_meta['first_contact_date'] = $post->post_date_gmt;
            }

            // Add epoch-millisecond date fields so ES can run date_histogram on DT data.
            // All _ms fields use epoch_millis — compatible with ES date type format "epoch_millis".
            if (!empty($flat_meta['first_contact_date'])) {
                $ts = strtotime($flat_meta['first_contact_date'] . ' UTC');
                if ($ts !== false) $flat_meta['first_contact_date_ms'] = $ts * 1000;
            }
            if (!empty($flat_meta['start_date']) && is_numeric($flat_meta['start_date'])) {
                $flat_meta['start_date_ms'] = (int)$flat_meta['start_date'] * 1000;
            }
            if (!empty($flat_meta['baptism_date']) && is_numeric($flat_meta['baptism_date'])) {
                $flat_meta['baptism_date_ms'] = (int)$flat_meta['baptism_date'] * 1000;
            }
            if (!empty($flat_meta['church_start_date']) && is_numeric($flat_meta['church_start_date'])) {
                $flat_meta['church_start_date_ms'] = (int)$flat_meta['church_start_date'] * 1000;
            }

            $doc = [
                'ID' => $post->ID,
                'post_type' => $post->post_type,
                'title' => $post->post_title,
                'content' => $post->post_content,
                'author' => $post->post_author,
                'date_created' => $post->post_date_gmt,
                'date_modified' => $post->post_modified_gmt,
                '@timestamp' => $export_timestamp,
                'team_title' => $team_title,
                'meta' => $flat_meta
            ];

            $ts = strtotime($post->post_date_gmt . ' UTC');
            if ($ts !== false) $doc['date_created_ms'] = $ts * 1000;

            $ts_mod = strtotime($post->post_modified_gmt . ' UTC');
            if ($ts_mod !== false) $doc['date_modified_ms'] = $ts_mod * 1000;

            $lines[] = json_encode(['index' => ['_index' => $index, '_id' => $post->ID]]);
            $lines[] = json_encode($doc);
        }
    }

    if (empty($lines)) {
        return new WP_Error('no_data', 'No content was found to export.');
    }

    $body = implode("
", $lines) . "
";

    $response = wp_remote_post($endpoint, [
        'headers' => [
            'Content-Type' => 'application/x-ndjson',
            'Authorization' => 'ApiKey ' . $api_key
        ],
        'body' => $body,
        'method' => 'POST',
        'timeout' => 60,
        'sslverify' => false
    ]);

    if (is_wp_error($response)) {
        return $response;
    }

    $code = wp_remote_retrieve_response_code($response);
    $response_body = wp_remote_retrieve_body($response);

    if ($code >= 400) {
        return new WP_Error('elk_error', "ELK API Error (HTTP $code) at $endpoint: " . $response_body);
    }

    $result = json_decode($response_body, true);
    if (!empty($result['errors'])) {
        $first_error = '';
        foreach ($result['items'] as $item) {
            $action = reset($item);
            if (!empty($action['error'])) {
                $first_error = json_encode($action['error']);
                break;
            }
        }
        return new WP_Error('elk_bulk_error', "ELK Bulk API reported errors. First error: " . $first_error);
    }

    return true;
}

function dtelk_setup_cron_export() {
    if (!wp_next_scheduled('dtelk_cron_export')) {
        wp_schedule_event(time(), 'twicedaily', 'dtelk_cron_export');
    }
}
add_action('init', 'dtelk_setup_cron_export');
add_action('dtelk_cron_export', 'dtelk_export_to_elk');

function dtelk_deactivate() {
    $timestamp = wp_next_scheduled('dtelk_cron_export');
    wp_unschedule_event($timestamp, 'dtelk_cron_export');
}
register_deactivation_hook(__FILE__, 'dtelk_deactivate');
