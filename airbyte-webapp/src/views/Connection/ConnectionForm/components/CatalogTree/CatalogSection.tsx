import React, { memo, useCallback, useMemo } from "react";
import { useToggle } from "react-use";
import styled from "styled-components";
import { FormikErrors, getIn, useField } from "formik";

import {
  AirbyteStreamConfiguration,
  DestinationSyncMode,
  getDestinationNamespace,
  SyncMode,
  SyncSchemaFieldObject,
  SyncSchemaStream,
} from "core/domain/catalog";
import { traverseSchemaToField } from "core/domain/catalog/fieldUtil";
import { DropDownRow } from "components";
import { TreeRowWrapper } from "./components/TreeRowWrapper";
import { Rows } from "./components/Rows";

import { ConnectionFormValues, SUPPORTED_MODES } from "../../formConfig";
import { StreamHeader } from "./StreamHeader";
import { FieldHeader } from "./FieldHeader";
import { FieldRow } from "./FieldRow";

import { equal, naturalComparatorBy } from "utils/objects";

const Section = styled.div<{ error?: boolean }>`
  border: 1px solid
    ${(props) => (props.error ? props.theme.dangerColor : "none")};
`;

type TreeViewRowProps = {
  streamNode: SyncSchemaStream;
  errors: FormikErrors<ConnectionFormValues>;
  destinationSupportedSyncModes: DestinationSyncMode[];
  updateStream: (
    id: string,
    newConfiguration: Partial<AirbyteStreamConfiguration>
  ) => void;
};

const CatalogSectionInner: React.FC<TreeViewRowProps> = ({
  streamNode,
  updateStream,
  errors,
  destinationSupportedSyncModes,
}) => {
  const [isRowExpanded, onExpand] = useToggle(false);
  const { stream, config } = streamNode;
  const streamId = stream.name;

  const updateStreamWithConfig = useCallback(
    (config: Partial<AirbyteStreamConfiguration>) =>
      updateStream(streamNode.id, config),
    [updateStream, streamNode]
  );

  const onSelectSyncMode = useCallback(
    (data: DropDownRow.IDataItem | null) => {
      data && updateStreamWithConfig(data.value);
    },
    [updateStreamWithConfig]
  );

  const onSelectStream = useCallback(
    () =>
      updateStreamWithConfig({
        selected: !config.selected,
      }),
    [config, updateStreamWithConfig]
  );

  const pkPaths = useMemo(
    () => new Set(config.primaryKey.map((pkPath) => pkPath.join("."))),
    [config.primaryKey]
  );

  const onPkSelect = useCallback(
    (pkPath: string[]) => {
      let newPrimaryKey: string[][];

      if (pkPaths.has(pkPath.join("."))) {
        newPrimaryKey = config.primaryKey.filter((key) => !equal(key, pkPath));
      } else {
        newPrimaryKey = [...config.primaryKey, pkPath];
      }

      updateStreamWithConfig({ primaryKey: newPrimaryKey });
    },
    [config.primaryKey, pkPaths, updateStreamWithConfig]
  );

  const onCursorSelect = useCallback(
    (cursorPath: string[]) =>
      updateStreamWithConfig({ cursorField: cursorPath }),
    [updateStreamWithConfig]
  );

  const pkRequired =
    config.destinationSyncMode === DestinationSyncMode.Dedupted;
  const cursorRequired = config.syncMode === SyncMode.Incremental;
  const showPkControl =
    stream.sourceDefinedPrimaryKey.length === 0 && pkRequired;
  const showCursorControl = !stream.sourceDefinedCursor && cursorRequired;

  const selectedCursorPath = config.cursorField.join(".");

  const fields = useMemo(() => {
    const traversedFields = traverseSchemaToField(stream.jsonSchema, streamId);

    return traversedFields.sort(
      naturalComparatorBy((field) => field.cleanedName)
    );
  }, [stream, streamId]);

  const availableSyncModes = useMemo(
    () =>
      SUPPORTED_MODES.filter(
        ([syncMode, destinationSyncMode]) =>
          stream.supportedSyncModes.includes(syncMode) &&
          destinationSupportedSyncModes.includes(destinationSyncMode)
      ).map(([syncMode, destinationSyncMode]) => ({
        value: { syncMode, destinationSyncMode },
      })),
    [stream.supportedSyncModes, destinationSupportedSyncModes]
  );

  const hasChildren = fields && fields.length > 0;

  const configErrors = getIn(errors, `schema.streams[${streamNode.id}].config`);
  const hasError = configErrors && Object.keys(configErrors).length > 0;

  const [{ value: namespaceDefinition }] = useField("namespaceDefinition");
  const [{ value: namespaceFormat }] = useField("namespaceFormat");
  const destNamespace = getDestinationNamespace({
    namespaceDefinition,
    namespaceFormat,
    sourceNamespace: stream.namespace,
  });

  const primitiveFields = fields
    .filter(SyncSchemaFieldObject.isPrimitive)
    .map((field) => field.name);

  return (
    <Section error={hasError}>
      <TreeRowWrapper>
        <StreamHeader
          stream={streamNode}
          destNamespace={destNamespace}
          destName={streamNode.stream.name}
          availableSyncModes={availableSyncModes}
          onSelectStream={onSelectStream}
          onSelectSyncMode={onSelectSyncMode}
          isRowExpanded={isRowExpanded}
          pkRequired={showPkControl}
          cursorRequired={showCursorControl}
          primitiveFields={primitiveFields}
          onPrimaryKeyChange={(newPrimaryKey) =>
            updateStreamWithConfig({ primaryKey: newPrimaryKey })
          }
          onCursorChange={onCursorSelect}
          hasFields={hasChildren}
          onExpand={onExpand}
        />
      </TreeRowWrapper>
      {isRowExpanded && hasChildren && (
        <>
          <TreeRowWrapper noBorder>
            <FieldHeader depth={1} />
          </TreeRowWrapper>
          <Rows fields={fields} depth={1}>
            {(field) => (
              <TreeRowWrapper depth={1}>
                <FieldRow
                  depth={1}
                  name={field.name}
                  type={field.type}
                  destinationName={field.cleanedName}
                  isCursor={field.name === selectedCursorPath}
                  isPrimaryKey={pkPaths.has(field.name)}
                  isPrimaryKeyEnabled={
                    showPkControl && SyncSchemaFieldObject.isPrimitive(field)
                  }
                  isCursorEnabled={
                    showCursorControl &&
                    SyncSchemaFieldObject.isPrimitive(field)
                  }
                  onPrimaryKeyChange={() => onPkSelect(field.name.split("."))}
                  onCursorChange={() => onCursorSelect(field.name.split("."))}
                />
              </TreeRowWrapper>
            )}
          </Rows>
        </>
      )}
    </Section>
  );
};

const CatalogSection = memo(CatalogSectionInner);

export { CatalogSection };
