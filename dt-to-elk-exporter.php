<?php
/**
 * Plugin Name: Disciple.Tools to ELK Exporter
 * Description: Exports Disciple.Tools data (contacts, groups, appointments, tasks) to ELK via Bulk API.
 * Version: 1.3
 * Author: Jon Ralls
 */

if (!defined('ABSPATH')) exit;

function dtelk_register_settings() {
    add_option('dtelk_elk_endpoint', '');
    add_option('dtelk_api_key', '');
    add_option('dtelk_index_name', '');
    register_setting('dtelk_options_group', 'dtelk_elk_endpoint');
    register_setting('dtelk_options_group', 'dtelk_api_key');
    register_setting('dtelk_options_group', 'dtelk_index_name');
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

function dtelk_export_to_elk() {
    $endpoint = rtrim(get_option('dtelk_elk_endpoint'), '/') . '/_bulk';
    $api_key = get_option('dtelk_api_key');
    $index = get_option('dtelk_index_name');

    if (!$endpoint || !$api_key || !$index) {
        return new WP_Error('missing_config', 'ELK settings (Endpoint, API Key, Index Name) are missing.');
    }

    $post_types = ['contacts', 'groups', 'dt_appointments', 'dt_tasks'];
    $lines = [];
    $ignored_meta_prefixes = ['contact'];
    $export_timestamp = gmdate('c');

    foreach ($post_types as $type) {
        $posts = get_posts([
            'post_type' => $type,
            'numberposts' => -1,
            'post_status' => 'any'
        ]);

        foreach ($posts as $post) {
            $meta = get_post_meta($post->ID);
            $flat_meta = [];
            foreach ($meta as $key => $value) {
                $skip = false;
                foreach ($ignored_meta_prefixes as $prefix) {
                    if (strpos($key, $prefix) === 0) {
                        $skip = true;
                        break;
                    }
                }
                if ($skip) continue;
                $flat_meta[$key] = maybe_unserialize($value[0]);
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
                'meta' => $flat_meta
            ];

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
    if ($code >= 400) {
        $error_body = wp_remote_retrieve_body($response);
        return new WP_Error('elk_error', "ELK API Error (HTTP $code): " . $error_body);
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
