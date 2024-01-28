# Dynamic Data Masking Framework
## Features:
- Scan the whole `dbt_fivetran` .yml files to gather sensitive fields with their masking_rules
- Mask sensitive data by value e.g. phone +84968985555 → 0000000000
- Mask sensitive data by hash functions (md5 or sha2) e.g.  84968985555 → 1ff697fc651722e857e1dbed53b91527
- Table owners can see their sensitive values
- Specific users e.g. `masteruser` , `dbt_cloud` can see all sensitive values
- The rest of users e.g. sisense can only see masked values
- CI/CD process integration: GitHub Actions can run and execute masking policies (create and attach) queries in Redshift after committing changes to `dbt_fivetran` yml files.

## Future works:
- Support partial masking
- Support conditional masking
- Optimize code and workflow

## How to use:
- In `*_source.yml` or `*_schema.yml` under `dbt_fivetran` folder, add `sensitive: true` in sensitive column meta
- For `masking_rules`, there are 2 options for `full_mask`:
    - `mask_function`: `md5` or `sha2-256` (Valid bits values for `sha2` are 0 (same as 256), 224, 256, 384, and 512.)
    - `mask_value`: the value to be showed instead of the original value
- [Sample config](https://github.com/Thinkei/gold-diggers/blob/main/dbt_fivetran/models/staging/outreach/_outreach_source.yml#L143):
  - ```yaml
        - name: users
          description: "The individual that uses the application."
          columns:
            - name: phone_number
              meta:
                sensitive: true
                masking_rules:
                  full_mask:
                    mask_function: md5
                    # mask_function: sha2-256
                    # mask_value: "0000000000"
- [Sample PR](https://github.com/Thinkei/gold-diggers/pull/332):
  - ```shell
      git checkout -b DAE-47-Mask-asana.user-sensitive-columns
      # Write code then git add
      git commit -m "DAE-47 🔒 Mask asana.user sensitive columns"
      git push origin DAE-47-Mask-asana.user-sensitive-columns
      # Peer review, approve then Squash and merge
      ```
- In order to mange users who can see all sensitive values, maintain this [role_config.yml](../../.github/governance/ddm/role_config.yml) file

# References
- [Jira issue][Jira issue]
- [Confluence][Confluence]
- [Source code](../../.github/governance/ddm)


<!-- links -->

[Jira issue]: https://employmenthero.atlassian.net/browse/DAE-15
[Confluence]: https://employmenthero.atlassian.net/wiki/spaces/Data/pages/2658828620/Dynamic+Data+Masking
