#pragma once

#include "flat_page_iface.h"
#include "flat_sausage_solid.h"
#include "flat_table_stats.h"
#include "flat_row_eggs.h"
#include "util_basics.h"
#include "shared_cache.h"

#include <ydb/library/actors/util/shared_data.h>

#include <util/generic/string.h>
#include <util/system/types.h>
#include <new>

namespace NKikimr {
namespace NTable {

    class TColdPart;
    class TPart;
    class TMemTable;

    using TPageId = NPage::TPageId;
    using TPageOffset = NPage::TPageOffset;
    using TPageLocation = NPage::TPageLocation;
    using EPage = NPage::EPage;

    struct ISaver {
        using TGlobId = NPageCollection::TGlobId;

        virtual void Save(TSharedData raw, NPage::TGroupId groupId) = 0;
        virtual TLargeObj Save(TRowId, ui32 tag, const TGlobId &glob) = 0;
        virtual TLargeObj Save(TRowId, ui32 tag, TArrayRef<const char> blob) = 0;
    };

    class IPageWriter {
    public:
        using EPage = NPage::EPage;
        using TPageId = NPage::TPageId;

        virtual ~IPageWriter() = default;
        virtual TPageLocation Write(TSharedData page, EPage type, ui32 group) = 0;
        virtual TPageId WriteOuter(TSharedData) = 0;
        virtual void WriteInplace(TPageId page, TArrayRef<const char> body) = 0;
        virtual NPageCollection::TGlobId WriteLarge(TString blob, ui64 ref) = 0;
        virtual void Finish(TString overlay) = 0;

        virtual ui32 GetLastWrittenPageId(ui32 group) const noexcept = 0;
    };

    struct IPages {
        using ELargeObj = NTable::ELargeObj;
        using TPart = NTable::TPart;
        using TMemTable = NTable::TMemTable;
        using TPageId = NPage::TPageId;
        using TPageLocation = NPage::TPageLocation;
        using TGroupId = NPage::TGroupId;

        virtual ~IPages() = default;

        struct TResult : private TMoveOnly {
            TResult(bool need, const TSharedData* data)
                : Need(need)
                , OwnsData_(false)
                , BorrowedData_(data)
            {
            }

            TResult(bool need, TSharedData&& data)
                : Need(need)
                , OwnsData_(true)
            {
                new (&Data_) TSharedData(std::move(data));
            }

            TResult(bool need, TSharedCachePageRef&& page)
                : TResult(need, page ? page.BuildSharedData() : TSharedData())
            {
                page.Drop();
            }

            TResult(TResult&& other) noexcept
                : Need(other.Need)
                , OwnsData_(other.OwnsData_)
            {
                MovePayload(other);
            }

            TResult& operator=(TResult&& other) noexcept {
                if (this != &other) {
                    DestroyPayload();
                    Need = other.Need;
                    OwnsData_ = other.OwnsData_;
                    MovePayload(other);
                }
                return *this;
            }

            ~TResult() {
                DestroyPayload();
            }

            explicit operator bool() const noexcept {
                return OwnsData_ ? bool(Data_) : bool(BorrowedData_);
            }

            const TSharedData* operator*() const noexcept {
                return OwnsData_ ? &Data_ : BorrowedData_;
            }

            bool Need;

        private:
            void MovePayload(TResult& other) noexcept {
                if (OwnsData_) {
                    new (&Data_) TSharedData(std::move(other.Data_));
                } else {
                    BorrowedData_ = std::exchange(other.BorrowedData_, nullptr);
                }
            }

            void DestroyPayload() noexcept {
                if (OwnsData_) {
                    Data_.~TSharedData();
                }
            }

            bool OwnsData_;

            union {
                const TSharedData* BorrowedData_;
                TSharedData Data_;
            };
        };

        static_assert(sizeof(TResult) == 24);

        virtual TResult Locate(const TMemTable*, ui64 ref, ui32 tag) = 0;
        virtual TResult Locate(const TPart*, ui64 ref, ELargeObj lob) = 0;
        virtual TSharedCachePageRef TryGetPage(const TPart* part, const TPageLocation& location, TGroupId groupId) = 0;

        /**
         * Hook for cleaning up env on DB.RollbackChanges()
         */
        virtual void OnRollbackChanges() {
            // nothing by default
        }
    };

}
}
