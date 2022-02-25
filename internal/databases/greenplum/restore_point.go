package greenplum

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/wal-g/wal-g/pkg/storages/storage"

	"github.com/spf13/viper"

	"github.com/blang/semver"
	"github.com/jackc/pgx"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/utility"
)

const RestorePointSuffix = "_restore_point.json"

type RestorePointMetadata struct {
	Name             string         `json:"name"`
	StartTime        time.Time      `json:"start_time"`
	FinishTime       time.Time      `json:"finish_time"`
	Hostname         string         `json:"hostname"`
	GpVersion        string         `json:"gp_version"`
	SystemIdentifier *uint64        `json:"system_identifier"`
	LsnBySegment     map[int]string `json:"lsn_by_segment"`
}

func (s *RestorePointMetadata) String() string {
	b, err := json.Marshal(s)
	if err != nil {
		return "-"
	}
	return string(b)
}

func RestorePointMetadataFileName(pointName string) string {
	return pointName + RestorePointSuffix
}

func FetchRestorePointMetadata(folder storage.Folder, pointName string) (RestorePointMetadata, error) {
	var restorePoint RestorePointMetadata
	err := internal.FetchDto(folder.GetSubFolder(utility.BaseBackupPath),
		&restorePoint, RestorePointMetadataFileName(pointName))
	if err != nil {
		return RestorePointMetadata{}, fmt.Errorf("failed to fetch metadata for restore point %s: %w", pointName, err)
	}

	return restorePoint, nil
}

// ValidateMatch checks that restore point is reachable from the provided backup
func ValidateMatch(folder storage.Folder, backupName string, restorePoint string) error {
	backup := NewBackup(folder, backupName)
	bSentinel, err := backup.GetSentinel()
	if err != nil {
		return fmt.Errorf("failed to fetch %s sentinel: %w", backupName, err)
	}

	rpMeta, err := FetchRestorePointMetadata(folder, restorePoint)
	if err != nil {
		tracelog.WarningLogger.Printf(
			"failed to fetch restore point %s metadata, will skip the validation check: %v", restorePoint, err)
		return nil
	}

	if bSentinel.FinishTime.After(rpMeta.FinishTime) {
		return fmt.Errorf("%s backup finish time (%s) is after the %s provided restore point finish time (%s)",
			backupName, bSentinel.FinishTime, restorePoint, rpMeta.FinishTime)
	}

	return nil
}

type RestorePointCreator struct {
	pointName        string
	startTime        time.Time
	systemIdentifier *uint64
	gpVersion        semver.Version

	Uploader *internal.Uploader
	Conn     *pgx.Conn

	logsDir string
	synchronized bool
}

// NewRestorePointCreator returns a restore point creator
func NewRestorePointCreator(pointName string, synchronized bool) (rpc *RestorePointCreator, err error) {
	uploader, err := internal.ConfigureUploader()
	if err != nil {
		return nil, err
	}

	conn, err := postgres.Connect()
	if err != nil {
		return nil, err
	}

	_, version, systemIdentifier, err := getGpClusterInfo(conn)
	if err != nil {
		return nil, err
	}

	rpc = &RestorePointCreator{
		pointName:        pointName,
		Uploader:         uploader,
		Conn:             conn,
		systemIdentifier: systemIdentifier,
		gpVersion:        version,
		logsDir:          viper.GetString(internal.GPLogsDirectory),
		synchronized:	  synchronized,
	}
	rpc.Uploader.UploadingFolder = rpc.Uploader.UploadingFolder.GetSubFolder(utility.BaseBackupPath)

	return rpc, nil
}

// Create creates cluster-wide consistent restore point
func (rpc *RestorePointCreator) Create() {
	rpc.startTime = utility.TimeNowCrossPlatformUTC()
	initGpLog(rpc.logsDir)

	err := rpc.checkExists()
	tracelog.ErrorLogger.FatalOnError(err)

	restoreLSNs, err := createRestorePoint(rpc.Conn, rpc.pointName)

	// TODO: fix this

	if rpc.synchronized {
		// Force WAL segment file switch on all segments
		err = gpSwitchWal()
		if err != nil {
			fmt.Printf("Could not switch WAL: %s\n", err)
			return nil, err
		}

		// Loop until the distributed restore point has been archived.
		for {
			ok, err := isRestorePointArchived(gpRestorePoint)
			if err != nil {
				shutdown(1)
			}
			if ok {
				break
			}
			// TODO: Implement better retry policy for not-yet archived case
			time.Sleep(5 * time.Second)
		}

		if verbosePrint {
			fmt.Printf("Distributed restore point archived: %s\n", gpRestorePoint.Name)
		}
	}
	tracelog.ErrorLogger.FatalOnError(err)

	err = rpc.uploadMetadata(restoreLSNs)
	if err != nil {
		tracelog.ErrorLogger.Printf("Failed to upload metadata file for restore point %s", rpc.pointName)
		tracelog.ErrorLogger.FatalError(err)
	}
	tracelog.InfoLogger.Printf("Restore point %s successfully created", rpc.pointName)
}

func createRestorePoint(conn *pgx.Conn, restorePointName string) (restoreLSNs map[int]string, err error) {
	tracelog.InfoLogger.Printf("Creating restore point with name %s", restorePointName)
	queryRunner, err := NewGpQueryRunner(conn)
	if err != nil {
		return
	}
	restoreLSNs, err = queryRunner.CreateGreenplumRestorePoint(restorePointName)
	if err != nil {
		return nil, err
	}
	return restoreLSNs, nil
}

func (rpc *RestorePointCreator) checkExists() error {
	exists, err := rpc.Uploader.UploadingFolder.Exists(RestorePointMetadataFileName(rpc.pointName))
	if err != nil {
		return fmt.Errorf("failed to check restore point existence: %v", err)
	}
	if exists {
		return fmt.Errorf("restore point with name %s already exists", rpc.pointName)
	}
	return nil
}

func (rpc *RestorePointCreator) uploadMetadata(restoreLSNs map[int]string) (err error) {
	hostname, err := os.Hostname()
	if err != nil {
		tracelog.WarningLogger.Printf("Failed to fetch the hostname for metadata, leaving empty: %v", err)
	}

	meta := RestorePointMetadata{
		Name:             rpc.pointName,
		StartTime:        rpc.startTime,
		FinishTime:       utility.TimeNowCrossPlatformUTC(),
		Hostname:         hostname,
		GpVersion:        rpc.gpVersion.String(),
		SystemIdentifier: rpc.systemIdentifier,
		LsnBySegment:     restoreLSNs,
	}

	metaFileName := RestorePointMetadataFileName(rpc.pointName)
	tracelog.InfoLogger.Printf("Uploading restore point metadata file %s", metaFileName)
	tracelog.InfoLogger.Println(meta.String())

	return internal.UploadDto(rpc.Uploader.UploadingFolder, meta, metaFileName)
}

// TODO: fix this

func gpSwitchWal() error {
	query := "SELECT * FROM pg_catalog.gp_switch_wal();"
	result := conn.PgConn().ExecParams(context.Background(), query, nil, nil, nil, nil).Read()
	if result.Err != nil {
		fmt.Printf("Error during WAL segment file switch: %s\n", result.Err)
		return result.Err
	}

	for _, row := range result.Rows {
		gpSegId, err := strconv.Atoi(string(row[0]))
		if err != nil {
			fmt.Printf("Could not parse gp_segment_id: %s\n", err)
			return err
		}

		walSwitchLsn, err := pgrepl.ParseLSN(string(row[1]))
		if err != nil {
			fmt.Printf("Could not parse pg_switch_wal: %s\n", err)
			return err
		}

		status[gpSegId].WalSwitchedLSN = walSwitchLsn
	}

	if verbosePrint {
		fmt.Println("Switched to the next WAL segment file")
	}

	return nil
}

func isRestorePointArchived(gprp rp.GPRestorePoint) (bool, error) {
	var gpSegmentIds []string
	var rpWals []string

	for _, rPoint := range gprp.RestorePoints {
		gpSegmentIds = append(gpSegmentIds, strconv.Itoa(rPoint.ContentId))
		rpWals = append(rpWals, fmt.Sprintf("pg_walfile_name('%s'::pg_lsn)",
			rPoint.Lsn.String()))
	}
	query := fmt.Sprintf(`
		SELECT rp_wal <= last_archived_wal AS rp_archived, *
		FROM
		gp_stat_archiver g
		INNER JOIN
		unnest(ARRAY[%s], ARRAY[%s]) AS u(gp_segment_id, rp_wal)
		ON g.gp_segment_id=u.gp_segment_id
		WHERE last_archived_wal IS NOT NULL
		ORDER BY g.gp_segment_id`,
		strings.Join(gpSegmentIds, ","),
		strings.Join(rpWals, ","))
	result := conn.PgConn().ExecParams(context.Background(), query, nil, nil, nil, nil).Read()

	if result.Err != nil {
		fmt.Printf("Error while checking archive status of GP restore point: %s\n", result.Err)
		return false, result.Err
	}

	// TODO: Log query and results at a low log level

	if len(result.Rows) != len(gpSegmentIds) {
		fmt.Printf("Not all of the segments have the GP restore point %s yet\n", gprp.Name)
		return false, nil
	}

	// Check to see if the restore point has been archived for every segment
	for _, row := range result.Rows {
		rpArchived, err := strconv.ParseBool(string(row[0]))
		if err != nil {
			fmt.Printf("Could not parse rp_archived: %s\n", err)
			return false, result.Err
		}
		if !rpArchived {
			return false, result.Err
		}
	}

	return true, nil
}
